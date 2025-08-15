import os, time, re, json, sys
from datetime import datetime, timezone
import requests
from supabase import create_client, Client

# ========= Boot log =========
print("ICEDCAPPWATCH COLLECTOR v3 — FK-safe (auto upsert items) ✅", file=sys.stderr)

# ========= Config depuis env =========
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
    sys.exit(1)
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

INTERVAL_MIN = int(os.environ.get("BATCH_INTERVAL_MINUTES", "20"))
BATCH_SIZE   = int(os.environ.get("BATCH_SIZE", "150"))
RATE         = float(os.environ.get("COLLECTOR_RATE_PER_SEC", "0.7"))

TIMS_GATEWAY_URL = os.environ.get("TIMS_GATEWAY_URL", "https://use1-prod-th-gateway.rbictg.com/graphql")
TIMS_AUTH   = os.environ.get("TIMS_AUTH", "")
TIMS_COOKIE = os.environ.get("TIMS_COOKIE", "")
TIMS_UA     = os.environ.get("TIMS_UA", "Mozilla/5.0")

TIMS_REGION       = os.environ.get("TIMS_REGION", "CA")
TIMS_CHANNEL      = os.environ.get("TIMS_CHANNEL", "whitelabel").lower()
TIMS_SERVICE_MODE = os.environ.get("TIMS_SERVICE_MODE", "pickup").lower()  # enum en minuscules

TIMS_HEADERS_JSON = os.environ.get("TIMS_HEADERS_JSON", "")
TIMS_EXTRA_VARIABLES_JSON = os.environ.get("TIMS_EXTRA_VARIABLES_JSON", "")

ITEM_PATTERNS = [p.strip() for p in os.environ.get(
    "ITEM_PATTERNS", r"iced\s*capp,capp[^a-zA-Z]{0,3}glac"
).split(",")]
PATTERNS = [re.compile(p, re.I) for p in ITEM_PATTERNS]

# ========= Helpers DB =========
def get_store_batch(offset: int, limit: int):
    # On lit depuis la vue "stores_official" pour ignorer les kgl_* synthétiques
    res = sb.table("stores_official").select("*").order("store_id").range(offset, offset + limit - 1).execute()
    return res.data or []
def upsert_item_basic(item_id: str, name: str | None):
    try:
        family = "iced_capp" if (name and any(p.search(name) for p in PATTERNS)) else None
        sb.table("items").upsert({
            "item_id": item_id,
            "name_en": name,
            "name_fr": name,
            "family": family
        }).execute()
    except Exception as e:
        # Non bloquant : si l'upsert échoue, on retentera à la prochaine passe
        print(f"[{item_id}] upsert_item failed: {e}", file=sys.stderr)

def upsert_check(store_id: str, item_id: str, is_available: bool, price_cents: int | None):
    payload = {
        "store_id": store_id,
        "item_id": item_id,
        "is_available": bool(is_available),
        "price_cents": price_cents,
        "checked_at": datetime.now(timezone.utc).isoformat()
    }
    try:
        sb.table("checks").insert(payload).execute()
        return
    except Exception as e:
        msg = str(e)
        # Si FK (23503) → insérer l'item puis réessayer une fois
        if "23503" in msg or "foreign key" in msg.lower():
            upsert_item_basic(item_id, None)
            try:
                sb.table("checks").insert(payload).execute()
                return
            except Exception as e2:
                print(f"[{store_id}] retry insert check failed: {e2}", file=sys.stderr)
                return
        print(f"[{store_id}] insert check failed: {e}", file=sys.stderr)

def get_store_batch(offset: int, limit: int):
    res = sb.table("stores").select("*").order("store_id").range(offset, offset + limit - 1).execute()
    return res.data or []

def refresh_materialized_view():
    try:
        sb.rpc("refresh_store_latest").execute()
    except Exception as e:
        print("Refresh MV failed (non-fatal):", e, file=sys.stderr)

# ========= GraphQL (POST) =========
def fetch_store_menu(store_id: str):
    # PosDataServiceMode! + enums en minuscules pour éviter les erreurs
    query = """query StoreMenu($storeId: ID!, $region: String!, $channel: Channel!, $serviceMode: PosDataServiceMode!) {
      storeMenu(storeId: $storeId, region: $region, channel: $channel, serviceMode: $serviceMode) {
        id
        isAvailable
        price { default }
      }
    }"""
    variables = {
        "storeId": store_id,
        "region": TIMS_REGION,
        "channel": TIMS_CHANNEL,
        "serviceMode": TIMS_SERVICE_MODE
    }
    if TIMS_EXTRA_VARIABLES_JSON:
        try:
            extra = json.loads(TIMS_EXTRA_VARIABLES_JSON)
            extra.pop("serviceMode", None)  # ne pas écraser
            variables.update(extra)
        except Exception as e:
            print("WARN bad TIMS_EXTRA_VARIABLES_JSON:", e, file=sys.stderr)

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "user-agent": TIMS_UA,
        "origin": "https://www.timhortons.ca",
        "referer": "https://www.timhortons.ca/",
    }
    if TIMS_HEADERS_JSON:
        try:
            headers.update(json.loads(TIMS_HEADERS_JSON))
        except Exception as e:
            print("WARN bad TIMS_HEADERS_JSON:", e, file=sys.stderr)
    if TIMS_AUTH:
        headers["authorization"] = TIMS_AUTH
    if TIMS_COOKIE:
        headers["cookie"] = TIMS_COOKIE

    try:
        r = requests.post(
            TIMS_GATEWAY_URL,
            json={"operationName": "StoreMenu", "variables": variables, "query": query},
            headers=headers,
            timeout=25
        )
    except requests.RequestException as e:
        raise RuntimeError(f"Gateway request failed: {e}")

    if r.status_code != 200:
        print("DEBUG gateway status:", r.status_code, file=sys.stderr)
        print("DEBUG gateway body:", r.text[:1000], file=sys.stderr)
        raise RuntimeError(f"Gateway HTTP {r.status_code}")

    data = r.json()
    if "errors" in data:
        print("DEBUG gql errors:", data["errors"], file=sys.stderr)
    return data.get("data", {}).get("storeMenu", [])

def map_item_name(item_id: str) -> str | None:
    try:
        res = sb.table("items").select("item_id,name_en,name_fr").eq("item_id", item_id).limit(1).execute()
        if res.data:
            row = res.data[0]
            return row.get("name_en") or row.get("name_fr")
    except Exception:
        pass
    return None

def looks_like_iced_capp(name: str | None) -> bool:
    if not name:
        return False
    return any(p.search(name) for p in PATTERNS)

# ========= Traitement d'un magasin =========
def process_store(store):
    store_id = store["store_id"]
    try:
        entries = fetch_store_menu(store_id)
    except Exception as e:
        print(f"[{store_id}] fetch error:", e, file=sys.stderr)
        return 0, 0

    hits = 0
    greens = 0
    for ent in entries:
        iid = (ent.get("id") or "").strip()
        avail = bool(ent.get("isAvailable"))
        price = None
        price_obj = ent.get("price") or {}
        if isinstance(price_obj, dict):
            price = price_obj.get("default")

        # 1) essaie d'avoir un nom depuis items (si on l'a déjà)
        name = map_item_name(iid)

        # 2) upsert l’item au besoin (nom inconnu → on met None, on complétera plus tard)
        upsert_item_basic(iid, name)

        # (facultatif) si tu veux filtrer strictement par nom connu:
        # if name and not looks_like_iced_capp(name):
        #     continue

        hits += 1
        if avail:
            greens += 1

        # 3) insert la check (FK-safe: si ça casse, on upsert item puis retry)
        upsert_check(store_id, iid or "unknown_item", avail, price)

    print(f"[{store_id}] items:{hits} green:{greens}")
    return hits, greens

# ========= Batch =========
def run_once():
    res = sb.table("stores").select("store_id", count="exact").execute()
    cnt = getattr(res, "count", None)
    if cnt is None:
        cnt = len(res.data or [])
    print("Total stores:", cnt)

    offset = 0
    while offset < cnt:
        batch = get_store_batch(offset, BATCH_SIZE)
        if not batch:
            break
        for s in batch:
            process_store(s)
            time.sleep(1.0 / max(RATE, 0.1))
        offset += len(batch)

    refresh_materialized_view()
    print("Batch done at", datetime.now(timezone.utc).isoformat())

def main():
    interval = max(INTERVAL_MIN, 1) * 60
    while True:
        try:
            run_once()
        except Exception as e:
            print("Batch error:", e, file=sys.stderr)
        time.sleep(interval)

if __name__ == "__main__":
    main()
