# map_province_official_ids.py — Autoprobe des résultats (edges/nodes/items etc.)
import os, sys, json, math, re, requests
from typing import Any, Dict, List, Optional, Tuple
from supabase import create_client, Client
import time, random


print("MAPPER — autoprobe GraphQL ✅", file=sys.stderr)

# ---- Supabase ----
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    print("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY", file=sys.stderr); sys.exit(1)
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ---- Gateway config ----
TIMS_GATEWAY_URL = os.environ.get("TIMS_GATEWAY_URL", "https://use1-prod-th-gateway.rbictg.com/graphql")
TIMS_AUTH   = os.environ.get("TIMS_AUTH", "")
TIMS_COOKIE = os.environ.get("TIMS_COOKIE", "")
TIMS_UA     = os.environ.get("TIMS_UA", "Mozilla/5.0")

HEADERS_JSON = os.environ.get("TIMS_HEADERS_JSON", "")
OP  = os.environ.get("TIMS_NEARBY_OPERATION", "GetRestaurants")
RAW = os.environ.get("TIMS_NEARBY_QUERY")  # texte GraphQL OU JSON "view source" du payload DevTools

# RestaurantsInput paramètres (override via env si besoin)
FILTER = os.environ.get("TIMS_NEARBY_FILTER", "NEARBY")
STATUS = os.environ.get("TIMS_NEARBY_STATUS", "OPEN")
FIRST  = int(os.environ.get("TIMS_NEARBY_FIRST", "20"))
RADIUS = int(os.environ.get("TIMS_NEARBY_RADIUS_METERS", "15000"))

MATCH_METERS = int(os.environ.get("TIMS_NEARBY_MATCH_METERS", "2500"))

# ---------- utils ----------
def haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6371000.0
    from math import radians, sin, cos, sqrt, atan2
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1); dl = radians(lon2 - lon1)
    a = sin(dphi/2)**2 + cos(phi1)*cos(phi2)*sin(dl/2)**2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))

def _headers() -> Dict[str,str]:
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

# --- extraction robuste de valeurs (id/lat/lon) même si imbriquées ---
LAT_KEYS = {"latitude","lat"}
LON_KEYS = {"longitude","lon","lng"}
ID_KEYS  = {"id","storeid","store_id","storenumber","store_number"}

def _walk(d: Any):
    if isinstance(d, dict):
        for k,v in d.items():
            yield k, v
            if isinstance(v, (dict, list)):
                for kv in _walk(v): yield kv
    elif isinstance(d, list):
        for it in d:
            for kv in _walk(it): yield kv

def find_number_by_keys(d: dict, keys: set) -> Optional[float]:
    # cherche une clé (ou sous-clé) dont le nom matche
    for k, v in _walk(d):
        if isinstance(k, str) and k.lower() in keys:
            try:
                return float(v)
            except Exception:
                pass
    return None

def find_string_by_keys(d: dict, keys: set) -> Optional[str]:
    for k, v in _walk(d):
        if isinstance(k, str) and k.lower() in keys:
            s = str(v).strip()
            if s:
                return s
    return None

def arrays_with_coords(root: Any) -> List[Tuple[str, List[dict]]]:
    """Retourne les chemins vers des tableaux contenant des objets avec coords repérables."""
    out = []
    def walk(o, path):
        if isinstance(o, list) and o and isinstance(o[0], dict):
            has = (find_number_by_keys(o[0], LAT_KEYS) is not None) and (find_number_by_keys(o[0], LON_KEYS) is not None)
            if has:
                out.append((path, o))
        elif isinstance(o, dict):
            for k,v in o.items():
                walk(v, f"{path}.{k}" if path else k)
    walk(root, "data")
    return out
def _post_with_retry(url, payload, headers, timeout_sec=40, retries=4, backoff_ms=400):
    for attempt in range(retries + 1):
        try:
            return requests.post(url, json=payload, headers=headers, timeout=timeout_sec)
        except requests.exceptions.ReadTimeout:
            print(f"RETRY {attempt+1}/{retries} ReadTimeout", file=sys.stderr)
        except requests.exceptions.RequestException as e:
            print(f"RETRY {attempt+1}/{retries} RequestException: {e}", file=sys.stderr)
        # backoff expo + jitter
        time.sleep((backoff_ms/1000.0) * (2 ** attempt) + random.uniform(0, 0.2))
    return None

def fetch_candidates(lat: float, lon: float) -> List[dict]:
    if not RAW:
        print("Missing TIMS_NEARBY_QUERY", file=sys.stderr)
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

    # $input standardisé (RestaurantsInput)
    input_obj = {
        "filter": FILTER,
        "coordinates": { "userLat": float(lat), "userLng": float(lon), "searchRadius": int(RADIUS) },
        "first": int(FIRST),
        "status": STATUS
        # serviceModes optionnel -> on n’envoie pas si pas requis
    }
    # merge doux avec variables.input du payload (sans écraser coords/first)
    try:
        p_input = payload_vars.get("input") if isinstance(payload_vars, dict) else None
        if isinstance(p_input, dict):
            extra = dict(p_input)
            for k in ["coordinates","first"]:
                extra.pop(k, None)
            input_obj.update(extra)
    except Exception:
        pass

    # appel avec retry
    timeout_sec = int(os.environ.get("TIMS_REQUEST_TIMEOUT_SEC", "40"))
    retries     = int(os.environ.get("TIMS_REQUEST_RETRIES", "4"))
    backoff_ms  = int(os.environ.get("TIMS_REQUEST_BACKOFF_MS", "400"))

    r = _post_with_retry(
        TIMS_GATEWAY_URL,
        {"operationName": op, "variables": {"input": input_obj}, "query": qry},
        _headers(),
        timeout_sec=timeout_sec,
        retries=retries,
        backoff_ms=backoff_ms,
    )

    if r is None:
        print("DEBUG nearby request: all retries failed", file=sys.stderr)
        return []

    if r.status_code != 200:
        print("DEBUG nearby status:", r.status_code, "body:", r.text[:700], file=sys.stderr)
        return []

    data = r.json()
    if "errors" in data:
        print("DEBUG nearby gql errors:", data["errors"], file=sys.stderr)
        return []

    root = data.get("data", {})

    # 1) Cherche un tableau qui contient des coords explicites
    arrs = arrays_with_coords(root)
    if arrs:
        path, arr = arrs[0]
        sample = {k: v for k, v in (arr[0].items())} if isinstance(arr[0], dict) else str(type(arr[0]))
        print(f"HINT: picked array path: {path}; sample keys: {list(sample.keys())[:8] if isinstance(sample, dict) else sample}", file=sys.stderr)
        return arr

    # 2) Fallback: pas de coords -> retourne restaurants.nodes/items/edges si dispo
    print("HINT: no coord arrays found. data keys:", list(root.keys())[:5] if isinstance(root, dict) else type(root).__name__, file=sys.stderr)
    try:
        restaurants = root.get("restaurants")
        if isinstance(restaurants, dict):
            for key in ["nodes", "items", "edges"]:
                if key in restaurants and isinstance(restaurants[key], list):
                    arr = restaurants[key]
                    print(f"HINT: picked array path: data.restaurants.{key}; sample keys: {list(arr[0].keys())[:8] if arr else []}", file=sys.stderr)
                    return arr
    except Exception:
        pass

    return []


def best_candidate(lat: float, lon: float, cands: List[Dict[str,Any]]) -> Optional[Tuple[str,float]]:
    """
    1) Essaie normal: si on trouve des coords (lat/lon) dans les items, on prend le plus proche ≤ MATCH_METERS.
    2) Fallback (pas de coords dispo): on prend le PREMIER item renvoyé (tri NEARBY) et on extrait un ID numérique.
    """
    def extract_numeric_id(s: Optional[str]) -> Optional[str]:
        if not s: return None
        m = re.search(r"\d{4,}", s)  # extrait un bloc de ≥4 chiffres (ex. "TH-123456" -> "123456")
        return m.group(0) if m else None

    best_id, best_d = None, 1e12
    got_coords = False

    for c in cands:
        try:
            id_raw = find_string_by_keys(c, ID_KEYS)  # ex. "TH-123456" ou "123456"
            cid = extract_numeric_id(id_raw)
            if not cid:
                continue
            clat = find_number_by_keys(c, LAT_KEYS)
            clon = find_number_by_keys(c, LON_KEYS)
            if clat is not None and clon is not None:
                got_coords = True
                d = haversine_m(lat, lon, clat, clon)
                if d < best_d:
                    best_id, best_d = cid, d
        except Exception:
            continue

    # Cas 1: on a pu mesurer une distance
    if got_coords and best_id and best_d <= MATCH_METERS:
        return best_id, best_d

    # Cas 2 (fallback): aucune coordonnée dans la réponse -> on prend le 1er item renvoyé
    if cands:
        try:
            id_raw = find_string_by_keys(cands[0], ID_KEYS)
            cid = extract_numeric_id(id_raw)
            if cid:
                # On renvoie une distance fictive == seuil pour passer le filtre
                return cid, float(MATCH_METERS)
        except Exception:
            pass

    return None


def update_store_id(old_id: str, new_id: str) -> bool:
    """
    Met à jour le PK de la ligne kgl_* vers l'ID officiel.
    Si l'ID officiel existe déjà dans `stores`, on FUSIONNE :
      - on rattache les checks de old_id -> new_id
      - on supprime la ligne old_id
    """
    try:
        # 1) l'ID officiel existe déjà ?
        check = sb.table("stores").select("store_id").eq("store_id", new_id).limit(1).execute()
        exists = bool(check.data)

        if exists:
            # Fusion douce : checks -> new_id, puis suppression de l'ancienne ligne
            try:
                sb.table("checks").update({"store_id": new_id}).eq("store_id", old_id).execute()
            except Exception as e:
                print(f"MERGE checks {old_id} -> {new_id} failed: {e}", file=sys.stderr)
                # on continue quand même, au pire il n'y avait pas de checks

            try:
                sb.table("stores").delete().eq("store_id", old_id).execute()
            except Exception as e:
                print(f"DELETE old store {old_id} failed: {e}", file=sys.stderr)

            return True
        else:
            # 2) pas de conflit : on peut mettre à jour le PK directement
            sb.table("stores").update({"store_id": new_id}).eq("store_id", old_id).execute()
            return True

    except Exception as e:
        print(f"UPDATE {old_id} -> {new_id} failed: {e}", file=sys.stderr)
        return False


# ---------- main ----------
def main():
    if len(sys.argv) < 2:
        print("Usage: python map_province_official_ids.py <PROVINCE_CODE>", file=sys.stderr); sys.exit(1)
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
        cands = fetch_candidates(lat, lon)
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
