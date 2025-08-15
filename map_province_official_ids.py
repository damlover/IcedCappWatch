# map_province_official_ids.py
# Mappe les store_id non officiels (kgl_*/non numériques) vers des IDs officiels
# en interrogeant le gateway pour les magasins proches (par lat/lon).

import os, sys, json, math, re, requests
from typing import Any, Dict, List, Optional, Tuple
from supabase import create_client, Client

print("MAPPER — province pilot (nearbyStores) ✅", file=sys.stderr)

# --- Config Supabase (mêmes variables que le collecteur) ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr); sys.exit(1)
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Config gateway (reprend celles de Railway) ---
TIMS_GATEWAY_URL = os.environ.get("TIMS_GATEWAY_URL", "https://use1-prod-th-gateway.rbictg.com/graphql")
TIMS_AUTH   = os.environ.get("TIMS_AUTH", "")
TIMS_COOKIE = os.environ.get("TIMS_COOKIE", "")
TIMS_UA     = os.environ.get("TIMS_UA", "Mozilla/5.0")

REGION  = os.environ.get("TIMS_REGION", "CA")
CHANNEL = os.environ.get("TIMS_CHANNEL", "whitelabel").lower()
SMODE   = os.environ.get("TIMS_SERVICE_MODE", "pickup").lower()

HEADERS_JSON = os.environ.get("TIMS_HEADERS_JSON", "")
EXTRA_VARS   = os.environ.get("TIMS_EXTRA_VARIABLES_JSON", "")

NEARBY_OPERATION = os.environ.get("TIMS_NEARBY_OPERATION", "NearbyStores")
NEARBY_QUERY = os.environ.get("TIMS_NEARBY_QUERY", """
query NearbyStores($region: String!, $channel: Channel!, $serviceMode: PosDataServiceMode!, $lat: Float!, $lon: Float!, $limit: Int){
  nearbyStores(region: $region, channel: $channel, serviceMode: $serviceMode, location: { latitude: $lat, longitude: $lon }, limit: $limit) {
    id
    latitude
    longitude
    distanceMeters
    address { city province line1 postalCode }
  }
}""").strip()

MATCH_METERS = int(os.environ.get("TIMS_NEARBY_MATCH_METERS", "400"))

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6371000.0
    from math import radians, sin, cos, sqrt, atan2
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1); dl = radians(lon2 - lon1)
    a = sin(dphi/2)**2 + cos(phi1)*cos(phi2)*sin(dl/2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))

def fetch_nearby(lat: float, lon: float, limit: int=5) -> List[Dict[str,Any]]:
    """
    Essaie d'abord d'utiliser la requête fournie via l'env:
      - TIMS_NEARBY_OPERATION
      - TIMS_NEARBY_QUERY   (colle ici la requête GraphQL exacte du HAR)
    Si non définies, on ne tente rien (pour éviter des 400 en boucle) et on t'affiche
    la liste des champs Query disponibles si l'introspection est permise.
    """
    headers = {
        "accept":"application/json",
        "content-type":"application/json",
        "user-agent": TIMS_UA,
        "origin":"https://www.timhortons.ca",
        "referer":"https://www.timhortons.ca/"
    }
    if HEADERS_JSON:
        try: headers.update(json.loads(HEADERS_JSON))
        except Exception as e: print("WARN bad TIMS_HEADERS_JSON:", e, file=sys.stderr)
    if TIMS_AUTH: headers["authorization"] = TIMS_AUTH
    if TIMS_COOKIE: headers["cookie"] = TIMS_COOKIE

    # 1) Si tu fournis la requête via les variables d'env, on l'utilise telle quelle
    op = os.environ.get("TIMS_NEARBY_OPERATION")
    qry = os.environ.get("TIMS_NEARBY_QUERY")
    if op and qry:
        variables = {
            "region": REGION,
            "channel": CHANNEL,
            "serviceMode": SMODE,
            "lat": float(lat),
            "lon": float(lon),
            "limit": int(limit)
        }
        if EXTRA_VARS:
            try:
                extra = json.loads(EXTRA_VARS)
                for k in ["lat","lon","region","channel","serviceMode"]:
                    extra.pop(k, None)
                variables.update(extra)
            except Exception as e:
                print("WARN bad TIMS_EXTRA_VARIABLES_JSON:", e, file=sys.stderr)

        r = requests.post(TIMS_GATEWAY_URL, json={
            "operationName": op,
            "variables": variables,
            "query": qry
        }, headers=headers, timeout=25)

        if r.status_code != 200:
            print("DEBUG nearby status:", r.status_code, "body:", r.text[:700], file=sys.stderr)
            return []

        data = r.json()
        if "errors" in data:
            print("DEBUG nearby gql errors:", data["errors"], file=sys.stderr)
            return []

        root = data.get("data", {})
        # essaie d’extraire le premier tableau retourné
        for v in root.values():
            if isinstance(v, list):
                return v
        return []

    # 2) Sinon: on tente une introspection pour tister les champs disponibles
    try:
        introspect = requests.post(
            TIMS_GATEWAY_URL,
            json={"query": "query __I{ __schema { queryType { fields { name } } } }"},
            headers=headers, timeout=15
        )
        if introspect.status_code == 200:
            info = introspect.json()
            fields = [f.get("name") for f in (info.get("data", {})
                                              .get("__schema", {})
                                              .get("queryType", {})
                                              .get("fields", []))]
            hints = [f for f in fields if any(k in f.lower() for k in ["near","store","location","search"])]
            print("HINT Query fields:", fields[:30], file=sys.stderr)
            print("HINT candidates:", hints, file=sys.stderr)
        else:
            print("Introspection not available (status", introspect.status_code, ")", file=sys.stderr)
    except Exception as e:
        print("Introspection error:", e, file=sys.stderr)

    return []


def best_candidate(lat: float, lon: float, cands: List[Dict[str,Any]]) -> Optional[Tuple[str,float]]:
    best = (None, 1e12)
    for c in cands:
        try:
            cid = str(c.get("id") or "").strip()
            clat = float(c.get("latitude")); clon = float(c.get("longitude"))
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
    # garder seulement ceux sans ID numérique officiel
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
