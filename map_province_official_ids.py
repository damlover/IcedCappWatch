# map_province_official_ids.py — RestaurantsInput compatible
import os, sys, json, math, re, requests
from typing import Any, Dict, List, Optional, Tuple
from supabase import create_client, Client

print("MAPPER — RestaurantsInput mode ✅", file=sys.stderr)

# ---- Supabase ----
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr); sys.exit(1)
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---- Gateway config (mêmes vars que le collector) ----
TIMS_GATEWAY_URL = os.environ.get("TIMS_GATEWAY_URL", "https://use1-prod-th-gateway.rbictg.com/graphql")
TIMS_AUTH   = os.environ.get("TIMS_AUTH", "")
TIMS_COOKIE = os.environ.get("TIMS_COOKIE", "")
TIMS_UA     = os.environ.get("TIMS_UA", "Mozilla/5.0")

HEADERS_JSON = os.environ.get("TIMS_HEADERS_JSON", "")
EXTRA_VARS   = os.environ.get("TIMS_EXTRA_VARIABLES_JSON", "")

# ---- Nearby via GetRestaurants($input: RestaurantsInput!) ----
OP  = os.environ.get("TIMS_NEARBY_OPERATION", "GetRestaurants")
RAW = os.environ.get("TIMS_NEARBY_QUERY")  # texte GraphQL OU JSON "view source" du payload DevTools

# Paramètres RestaurantsInput (env → override)
FILTER = os.environ.get("TIMS_NEARBY_FILTER", "NEARBY")           # ex: NEARBY
STATUS = os.environ.get("TIMS_NEARBY_STATUS", "OPEN")             # ex: OPEN
FIRST  = int(os.environ.get("TIMS_NEARBY_FIRST", "20"))           # ex: 20
RADIUS = int(os.environ.get("TIMS_NEARBY_RADIUS_METERS", "8000")) # ex: 8000
try:
    SERVICE_MODES = json.loads(os.environ.get("TIMS_NEARBY_SERVICE_MODES_JSON", '["pickup"]'))
    if not isinstance(SERVICE_MODES, list): SERVICE_MODES = ["pickup"]
except Exception:
    SERVICE_MODES = ["pickup"]

MATCH_METERS = int(os.environ.get("TIMS_NEARBY_MATCH_METERS", "400"))

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6371000.0
    from math import radians, sin, cos, sqrt, atan2
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1); dl = radians(lon2 - lon1)
    a = sin(dphi/2)**2 + cos(phi1)*cos(phi2)*sin(dl/2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))

def _base_headers() -> Dict[str,str]:
    h = {
        "accept":"application/json",
        "content-type":"application/json",
        "user-agent": TIMS_UA,
        "origin":"https://www.timhortons.ca",
        "referer":"https://www.timhortons.ca/",
    }
    if HEADERS_JSON:
        try: h.update(json.loads(HEADERS_JSON))
        except Exception as e: print("WARN bad TIMS_HEADERS_JSON:", e, file=sys.stderr)
    if TIMS_AUTH: h["authorization"] = TIMS_AUTH
    if TIMS_COOKIE: h["cookie"] = TIMS_COOKIE
    return h

def fetch_nearby(lat: float, lon: float, limit: int=5) -> List[Dict[str,Any]]:
    """
    Accepte:
      - TIMS_NEARBY_QUERY = texte GraphQL (query GetRestaurants($input: ...) { ... })
      - OU TIMS_NEARBY_QUERY = JSON brut du payload DevTools (objet/array "view source"),
        on extrait alors operationName/query/variables.input automatiquement.
    Construit $input conforme RestaurantsInput:
      { filter, coordinates{userLat,userLng,searchRadius}, first, status, serviceModes }
    """
    if not RAW:
        print("Missing TIMS_NEARBY_QUERY (texte GraphQL ou JSON DevTools)", file=sys.stderr)
        return []

    op  = OP
    qry = None
    payload_vars = {}

    raw = RAW
    if raw.strip()[:1] in "[{]":
        # JSON "view source" du payload DevTools
        try:
            blob = json.loads(raw)
            entry = blob[0] if isinstance(blob, list) and blob else (blob if isinstance(blob, dict) else None)
            if entry:
                op  = entry.get("operationName") or op
                qry = entry.get("query") or qry
                payload_vars = entry.get("variables") or {}
        except Exception as e:
            print("WARN TIMS_NEARBY_QUERY not valid JSON:", e, file=sys.stderr)
    else:
        qry = raw

    if not (op and qry):
        print("Missing operationName/query after parsing TIMS_NEARBY_QUERY", file=sys.stderr)
        return []

    # Build RestaurantsInput
    input_obj = {
        "filter": FILTER,
        "coordinates": { "userLat": float(lat), "userLng": float(lon), "searchRadius": int(RADIUS) },
        "first": int(FIRST),
        "status": STATUS,
        "serviceModes": SERVICE_MODES
    }

    # Merge doux avec variables.input du payload (sans écraser nos coords/first)
    try:
        p_input = payload_vars.get("input") if isinstance(payload_vars, dict) else None
        if isinstance(p_input, dict):
            extra = dict(p_input)
            for k in ["coordinates", "first"]:
                extra.pop(k, None)
            input_obj.update(extra)
    except Exception:
        pass

    headers = _base_headers()
    r = requests.post(
        TIMS_GATEWAY_URL,
        json={"operationName": op, "variables": {"input": input_obj}, "query": qry},
        headers=headers, timeout=25
    )

    if r.status_code != 200:
        print("DEBUG nearby status:", r.status_code, "body:", r.text[:700], file=sys.stderr)
        return []

    data = r.json()
    if "errors" in data:
        print("DEBUG nearby gql errors:", data["errors"], file=sys.stderr)
        return []

    root = data.get("data", {})
    # Attraper un tableau de magasins (restaurants/items/nodes)
    for v in root.values():
        if isinstance(v, list): return v
        if isinstance(v, dict):
            if "items" in v and isinstance(v["items"], list): return v["items"]
            if "nodes" in v and isinstance(v["nodes"], list): return v["nodes"]
    return []

def best_candidate(lat: float, lon: float, cands: List[Dict[str,Any]]) -> Optional[Tuple[str,float]]:
    best = (None, 1e12)
    for c in cands:
        try:
            cid = str(c.get("id") or c.get("storeId") or "").strip()
            clat = float(c.get("latitude") or c.get("lat"))
            clon = float(c.get("longitude") or c.get("lon"))
            d = haversine_m(lat, lon, clat, clon)
            if d < best[1]: best = (cid, d)
        except Exception:
            continue
    cid, dist = best
    if cid and re.fullmatch(r"\d+", cid) and dist <= MATCH_METERS:
        return cid, dist
    return None

def update_store_id(old_id: str, new_id: str) -> bool:
    try:
        sb.table("stores").update({"store_id": new_id}).eq("store_id", old_id).execute()
        return True
    except Exception as e:
        print(f"UPDATE {old_id} -> {new_id} failed: {e}", file=sys.stderr)
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python map_province_official_ids.py <PROVINCE_CODE>", file=sys.stderr)
        sys.exit(1)
    province = sys.argv[1].upper()
    print("Pilot province:", province)

    rows = sb.table("stores").select("store_id,name,address,city,province,lat,lon").eq("province", province).execute().data or []
    rows = [r for r in rows if not re.fullmatch(r"\d+", str(r.get("store_id") or ""))]

    print(f"À mapper (IDs non officiels) en {province}: {len(rows)}")
    mapped = 0
    for r in rows:
        sid = (r.get("store_id") or "").strip()
        lat, lon = r.get("lat"), r.get("lon")
        if lat is None or lon is None:
            print(f"- skip {sid} (no lat/lon)"); continue
        cands = fetch_nearby(lat, lon, limit=5)
        pick = best_candidate(lat, lon, cands)
        if not pick:
            print(f"- no match ≤{MATCH_METERS}m pour {sid} ({lat},{lon})"); continue
        new_id, dist = pick
        if update_store_id(sid, new_id):
            mapped += 1
            print(f"+ {sid} -> {new_id} (≈{int(dist)}m)")
    print(f"Fini. Mappé {mapped}/{len(rows)} pour {province}.")
    try:
        sb.rpc("refresh_store_latest").execute()
    except Exception:
        pass

if __name__ == "__main__":
    main()
