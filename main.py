import os, time, re, json, sys
from datetime import datetime, timezone
import requests
from supabase import create_client, Client

# ========= Config depuis les variables d'env =========
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
    sys.exit(1)
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Cadence / lot
INTERVAL_MIN = int(os.environ.get("BATCH_INTERVAL_MINUTES", "20"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "150"))
RATE = float(os.environ.get("COLLECTOR_RATE_PER_SEC", "0.7"))

# Gateway Tims (placeholders, à ajuster si besoin)
TIMS_GATEWAY_URL = os.environ.get("TIMS_GATEWAY_URL", "https://use1-prod-th-gateway.rbictg.com/graphql")
TIMS_AUTH = os.environ.get("TIMS_AUTH", "")           # ex: "Bearer eyJ..."
TIMS_COOKIE = os.environ.get("TIMS_COOKIE", "")       # ex: "session=..."
TIMS_UA = os.environ.get("TIMS_UA", "Mozilla/5.0")

# Entêtes / variables additionnelles injectables en JSON (clé/valeur)
# ex TIMS_HEADERS_JSON='{"x-country-code":"CA","accept-language":"fr-CA"}'
# ex TIMS_EXTRA_VARIABLES_JSON='{"channel":"WEB"}'
TIMS_HEADERS_JSON = os.environ.get("TIMS_HEADERS_JSON", "")
TIMS_EXTRA_VARIABLES_JSON = os.environ.get("TIMS_EXTRA_VARIABLES_JSON", "")

# Filtrage des items "Iced Capp"
ITEM_PATTERNS = [p.strip() for p in os.environ.get(
    "ITEM_PATTERNS",
    r"iced\s*capp,capp[^a-zA-Z]{0,3}glac"
).split(",")]
PATTERNS = [re.compile(p, re.I) for p in ITEM_PATTERNS]


# ========= Helpers DB =========
def get_store_batch(offset: int, limit: int):
    res = sb.table("stores").select("*").order("store_id").range(offset, offset + limit - 1).execute()
    return res.data or []

def upsert_check(store_id: str, item_id: str, is_available: bool, price_cents: int | None):
    sb.table("checks").insert({
        "store_id": store_id,
        "item_id": item_id,
        "is_available": bool(is_available),
        "price_cents": price_cents,
        "checked_at": datetime.now(timezone.utc).isoformat()
    }).execute()

def refresh_materialized_view():
    try:
        sb.rpc("refresh_store_latest").execute()
    except Exception as e:
        # non fatal (index unique requis pour CONCURRENTLY ; déjà géré côté SQL)
        print("Refresh MV failed (non-fatal):", e, file=sys.stderr)


# ========= Appel GraphQL (POST) =========
def fetch_store_menu(store_id: str):
    # storeMenu exige: storeId: ID!, region: String!, channel: Channel!, serviceMode: PosDataServiceMode
    query = """query StoreMenu($storeId: ID!, $region: String!, $channel: Channel!, $serviceMode: PosDataServiceMode) {
      storeMenu(storeId: $storeId, region: $region, channel: $channel, serviceMode: $serviceMode) {
        id
        isAvailable
        price { default }
      }
    }"""

    variables = {
        "storeId": store_id,
        "region": os.environ.get("TIMS_REGION", "CA"),
        "channel": os.environ.get("TIMS_CHANNEL", "whitelabel"),
        "serviceMode": os.environ.get("TIMS_SERVICE_MODE", "PICKUP")
    }

    # (facultatif) autres vars JSON sans toucher à serviceMode
    extra_vars = os.environ.get("TIMS_EXTRA_VARIABLES_JSON")
    if extra_vars:
        try:
            extra = json.loads(extra_vars)
            extra.pop("serviceMode", None)  # ne pas écraser
            variables.update(extra)
        except Exception:
            pass

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "user-agent": TIMS_UA,
        "origin": "https://www.timhortons.ca",
        "referer": "https://www.timhortons.ca/",
    }
    extra_headers = os.environ.get("TIMS_HEADERS_JSON")
    if extra_headers:
        try:
            headers.update(json.loads(extra_headers))
        except Exception:
            pass
    if TIMS_AUTH: headers["authorization"] = TIMS_AUTH
    if TIMS_COOKIE: headers["cookie"] = TIMS_COOKIE

    r = requests.post(
        TIMS_GATEWAY_URL,
        json={"operationName": "StoreMenu", "variables": variables, "query": query},
        headers=headers,
        timeout=25
    )
    if r.status_code != 200:
        print("DEBUG gateway status:", r.status_code, file=sys.stderr)
        print("DEBUG gateway body:", r.text[:1000], file=sys.stderr)
        raise RuntimeError(f"Gateway HTTP {r.status_code}")

    data = r.json()
    if "errors" in data:
        print("DEBUG gql errors:", data["errors"], file=sys.stderr)
    return data.get("data", {}).get("storeMenu", [])



# ========= Matching produit / name =========
def map_item_name(item_id: str) -> str | None:
    # Essaie de résoudre le nom depuis la table items (si on l'a peuplée)
    res = sb.table("items").select("item_id,name_en,name_fr").eq("item_id", item_id).limit(1).execute()
    if res.data:
        row = res.data[0]
        return row.get("name_en") or row.get("name_fr")
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
        iid = ent.get("id") or ""
        avail = bool(ent.get("isAvailable"))
        price = None
        price_obj = ent.get("price") or {}
        if isinstance(price_obj, dict):
            price = price_obj.get("default")

        name = map_item_name(iid)
        # Si on n'a pas le nom en base, on enregistre quand même la check (et on filtrera côté viz)
        if name and not looks_like_iced_capp(name):
            continue

        hits += 1
        if avail:
            greens += 1
        upsert_check(store_id, iid or "unknown_item", avail, price)

    print(f"[{store_id}] items:{hits} green:{greens}")
    return hits, greens


# ========= Batch complet =========
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
            # respect du rate-limit
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

