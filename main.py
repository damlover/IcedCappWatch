import os, time, re, json, sys
from datetime import datetime, timezone
import requests
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr)
    sys.exit(1)
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

INTERVAL_MIN = int(os.environ.get("BATCH_INTERVAL_MINUTES", "20"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "150"))
RATE = float(os.environ.get("COLLECTOR_RATE_PER_SEC", "0.7"))

TIMS_GATEWAY_URL = os.environ.get("TIMS_GATEWAY_URL", "https://use1-prod-th-gateway.rbictg.com/graphql")
TIMS_AUTH = os.environ.get("TIMS_AUTH", "")
TIMS_COOKIE = os.environ.get("TIMS_COOKIE", "")
TIMS_UA = os.environ.get("TIMS_UA", "Mozilla/5.0")

ITEM_PATTERNS = [p.strip() for p in os.environ.get("ITEM_PATTERNS","iced\\s*capp,capp[^a-zA-Z]{0,3}glac").split(",")]
PATTERNS = [re.compile(p, re.I) for p in ITEM_PATTERNS]

def get_store_batch(offset: int, limit: int):
    res = sb.table("stores").select("*").order("store_id").range(offset, offset+limit-1).execute()
    return res.data or []

def upsert_check(store_id: str, item_id: str, is_available: bool, price_cents: int|None):
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
        print("Refresh MV failed (non-fatal):", e, file=sys.stderr)

def fetch_store_menu(store_id: str):
    # Placeholder GraphQL call: adapte query/variables/headers selon le HAR
    query = "query StoreMenu($storeId:String!){ storeMenu(storeId:$storeId){ id isAvailable price { default } } }"
    variables = {"storeId": store_id}
    headers = { "content-type": "application/json", "user-agent": TIMS_UA }
    if TIMS_AUTH: headers["authorization"] = TIMS_AUTH
    if TIMS_COOKIE: headers["cookie"] = TIMS_COOKIE
    r = requests.get(TIMS_GATEWAY_URL, params={
        "operationName": "StoreMenu",
        "variables": json.dumps(variables),
        "query": query
    }, headers=headers, timeout=25)
    if r.status_code != 200:
        raise RuntimeError(f"Gateway HTTP {r.status_code}")
    data = r.json()
    return data.get("data", {}).get("storeMenu", [])

def map_item_name(item_id: str) -> str|None:
    res = sb.table("items").select("item_id,name_en,name_fr").eq("item_id", item_id).limit(1).execute()
    if res.data:
        row = res.data[0]
        return row.get("name_en") or row.get("name_fr")
    return None

def looks_like_iced_capp(name: str|None) -> bool:
    if not name: return False
    return any(p.search(name) for p in PATTERNS)

def process_store(store):
    store_id = store["store_id"]
    try:
        entries = fetch_store_menu(store_id)
    except Exception as e:
        print(f"[{store_id}] fetch error:", e, file=sys.stderr)
        return 0,0
    hits = 0
    greens = 0
    for ent in entries:
        iid = ent.get("id")
        avail = ent.get("isAvailable")
        price = None
        price_obj = ent.get("price") or {}
        if isinstance(price_obj, dict):
            price = price_obj.get("default")
        name = map_item_name(iid)
        if name and not looks_like_iced_capp(name):
            continue
        hits += 1
        if avail: greens += 1
        upsert_check(store_id, iid or "unknown_item", bool(avail), price)
    print(f"[{store_id}] items:{hits} green:{greens}")
    return hits, greens

def run_once():
    cnt = sb.table("stores").select("store_id", count="exact").execute().count or 0
    print("Total stores:", cnt)
    offset = 0
    while offset < cnt:
        batch = get_store_batch(offset, BATCH_SIZE)
        if not batch: break
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
