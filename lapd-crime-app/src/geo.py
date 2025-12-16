import json
from pathlib import Path
import requests

FEATURESERVER_QUERY_URL = (
    "https://firegis.lafd.org/arcgis/rest/services/Hosted/LAPD_Division/FeatureServer/5/query"
)

def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]

def geojson_path() -> Path:
    return _repo_root() / "assets" / "lapd_divisions.geojson"

def normalize_area_id(x) -> str:
    """
    Your gold area_id is a string. In the LAPD divisions layer, 'prec' is an integer.
    We normalize to string(int).
    """
    if x is None:
        return ""
    try:
        return str(int(float(x)))
    except Exception:
        return str(x).strip()

def load_or_fetch_lapd_geojson() -> dict:
    """
    Returns GeoJSON FeatureCollection with an added property:
      properties._area_id  (string)
    which we use as the Plotly join key.
    """
    p = geojson_path()
    if p.exists():
        with p.open("r", encoding="utf-8") as f:
            gj = json.load(f)
    else:
        params = {
            "where": "1=1",
            "outFields": "prec,aprec,name,bureau",
            "outSR": "4326",
            "f": "geojson",
            "returnGeometry": "true",
            "resultRecordCount": "2000",
        }
        r = requests.get(FEATURESERVER_QUERY_URL, params=params, timeout=60)
        r.raise_for_status()
        gj = r.json()

        p.parent.mkdir(parents=True, exist_ok=True)
        try:
            with p.open("w", encoding="utf-8") as f:
                json.dump(gj, f)
        except Exception:
            pass

    for feat in gj.get("features", []):
        props = feat.setdefault("properties", {})
        props["_area_id"] = normalize_area_id(props.get("prec"))

        props["_area_name"] = (props.get("aprec") or props.get("name") or "").strip()

    return gj
