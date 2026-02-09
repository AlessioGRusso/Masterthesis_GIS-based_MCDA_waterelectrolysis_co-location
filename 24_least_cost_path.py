
from pathlib import Path
import os, json, re, math
import warnings

# Warnungen unterdrücken - die sind nur störend
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings(
    "ignore",
    message=r"^Passed SRS uses EPSG:\d+ identification.*",
    category=RuntimeWarning,
    module=r"pyogrio\.raw",
)
warnings.filterwarnings(
    "ignore",
    message=r".*`square` is deprecated.*",
    category=FutureWarning,
    module=r"skimage\.morphology.*",
)

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, box, Polygon, MultiPolygon
from shapely.ops import unary_union, nearest_points
from shapely import wkb as _wkb
import rasterio
from rasterio.windows import from_bounds
from rasterio.transform import rowcol, xy
from rasterio import features
from rasterio.vrt import WarpedVRT
from rasterio.io import MemoryFile
from skimage.graph import MCP_Geometric, route_through_array
from skimage.morphology import binary_closing, remove_small_holes
import numpy as _np
import fiona
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ProcessPoolExecutor, as_completed

# Kompatibles Quadrat-Structuring-Element für verschiedene skimage-Versionen
try:
    from skimage.morphology import footprint_rectangle as _rect
    def SQUARE_FOOTPRINT(n: int): return _rect((n, n))
except Exception:
    try:
        from skimage.morphology import square as _sq
        def SQUARE_FOOTPRINT(n: int): return _sq(n)
    except Exception:
        def SQUARE_FOOTPRINT(n: int): return _np.ones((n, n), dtype=bool)

# =============================================================================
# Projektstruktur
# =============================================================================
ROOT    = Path(__file__).resolve().parent
DATA    = ROOT / "Daten"
OUT_DIR = ROOT / "Output" / "Geopackages"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# WWTP-Polygone als Quelle für Startpunkte (vor OSM-Snapping)
WWTP_SHAPES_GPKG = ROOT / "Output" / "WWTP Geopackages" / "WWTPS_Shapes.gpkg"
# Free Area (außerhalb Schutzgebiete) als Quelle für Startpunkte
FREE_AREA_GPKG = ROOT / "Daten" / "WWTP_Free_Area.gpkg"

WWTP_XLSX     = ROOT / "Output" / "UWWTD_TP_Database.xlsx"   # liest: General Data + Pipelines, schreibt: Pipelines

# CLC+ GeoTIFF (10 m, EPSG:3035)
RASTER = Path(r"D:\CLC\Results\CLMS_CLCplus_RASTER_2018_010m_eu_03035_V1_1\CLMS_CLCplus_RASTER_2018_010m_eu_03035_V1_1\Data\CLMS_CLCplus_RASTER_2018_010m_eu_03035_V1_1.tif")

# =============================================================================
# Basis-Kosten (CLC+ Klassen → Kosten)
# =============================================================================
WATER_COST_DEFAULT = 10  # teuer, aber endlich (10 m Zelle)

BASE_COSTS = {
    0: "inf",
    1: 3.0,   # Sealed (allgemein bebaut)
    2: 1.4,   # Coniferous
    3: 1.4,   # Broadleaved deciduous
    4: 1.4,   # Broadleaved evergreen
    5: 1.0,   # Shrubs
    6: 1.0,   # Permanent Herb
    7: 1.0,   # Periodically Herb
    8: 1.0,   # Lichens & mosses
    9: 1.0,   # Sparsely vegetated
    10: WATER_COST_DEFAULT,  # Water (teuer, nicht ∞)
    11: "inf",  # Snow & Ice
    254: "inf"
}

BUFFER_M  = 2500  # Routingfenster

# Wasser-Kontinuität reparieren
SEAL_WATER_GAPS   = True
WATER_CLOSE_SIZE  = 3
WATER_HOLE_AREA   = 12

# =============================================================================
# Schutzgebiete: ABSOLUTPREISE pro Klasse
# =============================================================================
USE_PROTECTED_AREAS    = True
PA_APPLY_TO_WATER      = False

PA_CLASS_ABS_COSTS = {
    "N2K_SPA":   100.0,
    "N2K_SCI":   100.0,
    "N2K_SAC":   100.0,
    "N2K_pSCI":  100.0,
    "N2K_OTHER": 3.0,
    "NATDA_WATER": 100.0,
    "NATDA_Ia":    100.0, "NATDA_Ib": 100.0, "NATDA_II": 100.0, "NATDA_III": 6.0,
    "NATDA_IV":    3.0, "NATDA_V":  1.8, "NATDA_VI": 1.3,
    "NATDA_OTHER": 3.0,
    "DEFAULT":     1.0,
}

PA_SRC_NATDA = {
    "tag": "NATDA",
    "service": "https://bio.discomap.eea.europa.eu/arcgis/rest/services/ProtectedSites/NatDAv22_Dyna_WM/MapServer",
    "layers": None
}
PA_SRC_N2K = {
    "tag": "N2K",
    "service": "http://bio.discomap.eea.europa.eu/arcgis/rest/services/ProtectedSites/Natura2000_Dyna_WM/MapServer",
    "layers": None
}
PA_REMOTE_SOURCES = [PA_SRC_NATDA, PA_SRC_N2K]

PA_WATER_KEYWORDS = [
    r"wasserschutz", r"trinkwasser", r"grundwasser", r"quellschutz",
    r"wellhead\s*protection", r"(drinking|potable)\s*water.*(protection|safeguard|zone)",
    r"groundwater.*protection", r"water\s*protection\s*zone", r"safeguard\s*zone"
]
PA_WATER_RE = re.compile("|".join(PA_WATER_KEYWORDS), re.I)
NATDA_DESIG_FIELDS = ["DESIG_ENG","DESIGNATION","designation","desig_eng","desigType",
                      "designatedAreaType","nationalDesignation","designationType","legalDesignation",
                      "siteName","SITENAME","NAME"]
NATDA_CAT_FIELDS = ["iucnCategory", "IUCN_CAT", "IUCNCATEGORY", "iucnCat", "IUCN"]
N2K_TYPE_FIELDS  = ["SITETYPE", "TYPE", "SiteType", "siteType", "DESIGNATION", "DESIG_ENG", "DESIG_TYPE"]

# =============================================================================
# OSM / Overpass — Snapping & Overlays
# =============================================================================
ENABLE_OSM_SNAPPING       = True
OSM_SNAP_RADII_M          = [500, 1500, 3000]
OSM_SNAP_TIMEOUT_S        = 25
OSM_BASE_URL              = "https://overpass-api.de/api/interpreter"

ENABLE_OSM_OVERLAYS       = True
OSM_OVERLAYS_IN_PARALLEL  = True
OSM_OVERLAYS_TIMEOUT_S    = 40


USE_OSM_FOR_HYDROGEN      = False

# Straßen/Gebäude
ROAD_ABS_COST             = 4.0
ROAD_BUFFER_M             = 6.0
ROAD_APPLY_ON_WATER       = False

BUILDING_IMPASSABLE       = True
START_ALLOW_BUILDING_M    = 200

# =============================================================================
# Slope / DEM — Zuschlagskosten (lokal oder WCS)
# =============================================================================
USE_SLOPE = True
DEM_PATH = r""   # leer: WCS nutzen
SLOPE_COST_PER_DEG = 0.25
SLOPE_MAX_DEG = 25.0
SLOPE_IGNORE_WATER = True

# DEM via Copernicus Data Space (Sentinel Hub WCS)
USE_WCS_FOR_DEM = True
CDSE_USERNAME    = os.getenv("CDSE_USERNAME", "alessio.grupe@stud.uni-hannover.de")
CDSE_PASSWORD    = os.getenv("CDSE_PASSWORD", "VhByV:6pH8PFYb_")
CDSE_INSTANCE_ID = os.getenv("CDSE_INSTANCE_ID", "5e311d65-12f4-48e2-a4bb-e158af5a4de7")
CDSE_WCS_LAYER   = os.getenv("CDSE_WCS_LAYER", "DEM_30")
CDSE_TOKEN_URL   = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CDSE_WCS_BASE    = "https://sh.dataspace.copernicus.eu/ogc/wcs"

# =============================================================================
#
# =============================================================================
PREFER_EXISTING_ROUTES = True
RECOMPUTE_IF_MISSING   = False

# =============================================================================
# HTTP Sessions
# =============================================================================
def _make_session(agent: str, retries_total=4, backoff=0.4, pool=8) -> requests.Session:
    s = requests.Session()
    r = Retry(total=retries_total, backoff_factor=backoff,
              status_forcelist=[429,500,502,503,504], allowed_methods=["GET","POST"])
    a = HTTPAdapter(max_retries=r, pool_connections=pool, pool_maxsize=pool)
    s.headers.update({"User-Agent": agent})
    s.mount("https://", a); s.mount("http://", a)
    return s

_PA_SESSION  = _make_session("WWTP-LeastCost/PA-Client")
_OSM_SESSION = _make_session("WWTP-LeastCost/Overpass")

_CDSE_TOKEN = None
def _cdse_get_token():
    global _CDSE_TOKEN
    if not (CDSE_USERNAME and CDSE_PASSWORD):
        return None
    try:
        r = requests.post(
            CDSE_TOKEN_URL,
            data={"grant_type":"password","client_id":"cdse-public",
                  "username":CDSE_USERNAME, "password":CDSE_PASSWORD},
            timeout=40,
        )
        if r.ok:
            _CDSE_TOKEN = r.json().get("access_token")
            return _CDSE_TOKEN
    except Exception:
        pass
    return None

def _wcs_get_dem_window(bounds, out_w, out_h, epsg):
    if not USE_WCS_FOR_DEM or not CDSE_INSTANCE_ID or not CDSE_WCS_LAYER:
        return None
    token = _cdse_get_token()
    if not token:
        return None
    params = {
        "SERVICE": "WCS",
        "REQUEST": "GetCoverage",
        "VERSION": "1.0.0",
        "COVERAGE": CDSE_WCS_LAYER,
        "CRS": f"EPSG:{int(epsg)}",
        "BBOX": f"{bounds[0]},{bounds[1]},{bounds[2]},{bounds[3]}",
        "WIDTH": str(int(out_w)),
        "HEIGHT": str(int(out_h)),
        "FORMAT": "image/geotiff",
    }
    url = f"{CDSE_WCS_BASE}/{CDSE_INSTANCE_ID}"
    try:
        r = _PA_SESSION.get(url, params=params,
                            headers={"Authorization": f"Bearer {token}"}, timeout=120)
        if not r.ok:
            return None
        with MemoryFile(r.content) as mem:
            with mem.open() as ds:
                arr = ds.read(1, masked=True)
                return arr
    except Exception:
        return None

# =============================================================================
# Parallelisierung
# =============================================================================
ENABLE_PARALLEL     = True
MAX_WORKERS         = max(1, min(2, (os.cpu_count() or 2) - 1))  # Reduziert von 4 auf 2 wegen Memory
PARALLEL_THRESHOLD  = 12

_POOL_SRC = None
_POOL_TARGETS = None
_POOL_SCENARIO = None
_POOL_WWTPS = None

def _pool_init(scenario_name: str, raster_path: str, wwtps_gdf=None):
    global _POOL_SRC, _POOL_TARGETS, _POOL_SCENARIO, _POOL_WWTPS
    _POOL_SCENARIO = scenario_name
    _POOL_WWTPS = wwtps_gdf
    _POOL_SRC = rasterio.open(raster_path)
    _POOL_TARGETS = load_targets(scenario_name, _POOL_SRC.crs, wwtps_gdf=_POOL_WWTPS)

# =============================================================================
# Helper & Utilities
# =============================================================================
def log(msg): print(msg, flush=True)

def progress_bar(proc: int, total: int, ok: int, ok_mcp: int, ok_fb: int, width: int = 32, end: bool=False):
    pct = 0.0 if total == 0 else proc / total
    filled = int(width * pct)
    bar = "█" * filled + "─" * (width - filled)
    msg = f"\r[{bar}] {pct*100:5.1f}%  verarbeitet: {proc}/{total}  OK: {ok}/{total} (MCP:{ok_mcp}, FB:{ok_fb})"
    print(msg, end=("" if not end else "\n"), flush=True)

def norm(s: str) -> str:
    return "".join(ch for ch in str(s).lower() if ch.isalnum())

def build_cost_lut(nodata, water_override=None):
    costs = dict(BASE_COSTS)
    if water_override is not None:
        costs[10] = float(water_override)
    lut = np.full(256, np.inf, dtype="float32")
    for k, v in costs.items():
        lut[int(k)] = (np.inf if str(v).lower() == "inf" else float(v))
    if nodata is not None:
        try: lut[int(nodata)] = np.inf
        except Exception: pass
    return lut

def nearest_point_on_targets(pt: Point, targets_gdf: gpd.GeoDataFrame) -> Point:
    u = unary_union(targets_gdf.geometry.values)
    return nearest_points(pt, u)[1]

def read_cost_window(src, bounds, lut):
    win  = from_bounds(*bounds, transform=src.transform)
    arr  = src.read(1, window=win, boundless=True, fill_value=src.nodata)
    cost = lut[arr]
    return cost, win, arr

def rc_from_xy(transform, x, y):
    r, c = rowcol(transform, x, y)
    return int(r), int(c)

def line_from_path(transform, window, path_rc, pt=None, tgt=None):
    if path_rc is None or len(path_rc) < 2:
        if pt is not None and tgt is not None:
            if (pt.x == tgt.x) and (pt.y == tgt.y):
                eps = 0.01
                return LineString([(pt.x, pt.y), (pt.x + eps, pt.y)])
            else:
                return LineString([(pt.x, pt.y), (tgt.x, tgt.y)])
        raise ValueError("Path too short and no pt/tgt provided.")
    pts = [xy(transform, r + int(window.row_off), c + int(window.col_off), offset="center")
           for (r, c) in path_rc]
    return LineString(pts)

def find_built_col(cols, n: int):
    target = f"builtscenario{n}"
    for c in cols:
        cs = norm(c)
        if target == cs or target in cs:
            return c
    return None

def find_distance_col(cols, key: str):
    keyn = norm(f"direct distance to {key}")
    for c in cols:
        if keyn == norm(c) or keyn in norm(c):
            return c
    alts = {"ehb":["distance to ehb","directdistanceehb"]}
    for alt in alts.get(key.lower(), []):
        for c in cols:
            if norm(alt) in norm(c):
                return c
    return None

def choose_join_key(cols_a, cols_b):
    for k in ["UWWTD Code","WWTP_ID","Plant_ID","ID","Id","id","Name","name"]:
        if k in cols_a and k in cols_b: return k
    return None

def resolve_ehb_files():
    trans = list(DATA.glob("*Transmission*.json"))
    hp1   = list(DATA.glob("*High*Pressure*Distribut*.json"))
    hp2   = list(DATA.glob("*High*Pressure*Distribiut*.json"))
    files = []
    if trans: files.append(trans[0])
    if hp1:   files.append(hp1[0])
    elif hp2: files.append(hp2[0])
    return files

# -------------------- Linien-Helper: ersten 5 km abschneiden ------------------
def _line_first_km(line: LineString, km: float = 5.0) -> LineString:
    target = float(km) * 1000.0
    if not isinstance(line, LineString):
        return line
    total = line.length
    if total <= target:
        return line
    coords = list(line.coords)
    acc = 0.0
    out = [coords[0]]
    for i in range(1, len(coords)):
        x0,y0 = coords[i-1]; x1,y1 = coords[i]
        seg = math.hypot(x1-x0, y1-y0)
        if acc + seg < target:
            out.append((x1,y1))
            acc += seg
        else:
            t = (target - acc) / seg if seg > 0 else 0.0
            x = x0 + t*(x1-x0); y = y0 + t*(y1-y0)
            out.append((x,y))
            break
    return LineString(out)

# =============================================================================
# Schutzgebiets-Helpers (Klassifizierung)
# =============================================================================
def _arc_envelope(bounds, wkid=3035):
    minx, miny, maxx, maxy = bounds
    return {"xmin": minx, "ymin": miny, "xmax": maxx, "ymax": maxy, "spatialReference": {"wkid": wkid}}

def _arcgis_list_polygon_layers(service_url: str) -> list[int]:
    try:
        r = _PA_SESSION.get(f"{service_url.rstrip('/')}", params={"f": "json"}, timeout=60)
        r.raise_for_status()
        j = r.json()
        layers = j.get("layers", []) or []
        ids = [int(l["id"]) for l in layers if str(l.get("geometryType","")).lower().endswith("polygon")]
        return ids if ids else [int(l["id"]) for l in layers]
    except Exception:
        return []

def _arcgis_query_layer_attrs(service_url: str, layer_id: int, bounds, out_wkid=3035) -> gpd.GeoDataFrame:
    base = f"{service_url.rstrip('/')}/{layer_id}/query"
    params = {
        "f": "geojson",
        "where": "1=1",
        "returnGeometry": "true",
        "geometryType": "esriGeometryEnvelope",
        "spatialRel": "esriSpatialRelIntersects",
        "geometry": json.dumps(_arc_envelope(bounds, wkid=out_wkid)),
        "inSR": out_wkid, "outSR": out_wkid,
        "outFields": "*"
    }
    r = _PA_SESSION.get(base, params=params, timeout=60)
    if not r.ok:
        return gpd.GeoDataFrame(geometry=[], crs=f"EPSG:{out_wkid}")
    try:
        gj = r.json()
    except Exception:
        return gpd.GeoDataFrame(geometry=[], crs=f"EPSG:{out_wkid}")
    if not isinstance(gj, dict) or gj.get("type") != "FeatureCollection":
        return gpd.GeoDataFrame(geometry=[], crs=f"EPSG:{out_wkid}")
    gdf = gpd.GeoDataFrame.from_features(gj)

    if hasattr(gdf, 'geometry') and 'geometry' in gdf.columns and gdf['geometry'].notna().any():
        try:
            gdf = gdf.set_geometry('geometry', crs=None)
        except Exception:
            pass
        try:
            gdf = gdf.set_crs(f"EPSG:{out_wkid}", allow_override=True)
        except Exception:
            # fall back to returning as-is
            return gdf
        return gdf

    return gpd.GeoDataFrame(geometry=gpd.GeoSeries([], crs=f"EPSG:{out_wkid}"))

def _natda_class(row) -> list[str]:
    text = ""
    for f in NATDA_DESIG_FIELDS:
        if f in row and pd.notna(row[f]):
            text += " " + str(row[f])
    if text and PA_WATER_RE.search(text):
        return ["NATDA_WATER"]
    val = None
    for f in NATDA_CAT_FIELDS:
        if f in row and pd.notna(row[f]):
            val = str(row[f]).strip()
            break
    if not val:
        return ["NATDA_OTHER"]
    v = val.upper().replace(" ", "")
    for k in ["IA","IB","II","III","IV","V","VI"]:
        if k == v or v.endswith(k):
            return [f"NATDA_{k}"]
    return ["NATDA_OTHER"]

def _n2k_class(row) -> list[str]:
    text = ""
    for f in N2K_TYPE_FIELDS:
        if f in row and pd.notna(row[f]):
            text = str(row[f]).upper()
            break
    classes = set()
    if "SPA" in text: classes.add("N2K_SPA")
    if "SAC" in text: classes.add("N2K_SAC")
    if "SCI" in text: classes.add("N2K_SCI")
    if "PSCI" in text: classes.add("N2K_pSCI")
    if text in ("A","C"): classes.add("N2K_SPA")
    if text in ("B","C"): classes.add("N2K_SCI")
    if not classes:
        classes.add("N2K_OTHER")
    return list(classes)

def _protected_classes_for_window(bounds, out_epsg=3035):
    shapes_by_class = {}
    all_shapes = []
    for src in PA_REMOTE_SOURCES:
        srv = src["service"]; tag = src["tag"]
        lids = _arcgis_list_polygon_layers(srv) if src.get("layers") is None else src["layers"]
        for lid in lids:
            g = _arcgis_query_layer_attrs(srv, lid, bounds, out_wkid=out_epsg)
            if g.empty:
                continue
            for _, row in g.iterrows():
                geom = row.geometry
                if geom is None or geom.is_empty:
                    continue
                classes = _n2k_class(row) if tag == "N2K" else _natda_class(row)
                for cls in classes:
                    shapes_by_class.setdefault(cls, []).append(geom)
                all_shapes.append(geom)
    return shapes_by_class, all_shapes

# =============================================================================
# Overpass: Snapping & Overlays
# =============================================================================
def _bbox_to_wgs84(bounds, crs):
    poly = gpd.GeoSeries([box(*bounds)], crs=crs).to_crs(4326).iloc[0]
    minx, miny, maxx, maxy = poly.bounds
    return (miny, minx, maxy, maxx)  # s, w, n, e

def _overpass(query: str, timeout_s=30):
    try:
        r = _OSM_SESSION.post(OSM_BASE_URL, data={"data": query}, timeout=timeout_s)
        if not r.ok:
            return None
        return r.json()
    except Exception:
        return None

def osm_snap_wwtp(lon: float, lat: float):
    for R in OSM_SNAP_RADII_M:
        q = f"""
        [out:json][timeout:{OSM_SNAP_TIMEOUT_S}];
        (
          nwr(around:{R},{lat},{lon})["man_made"="wastewater_plant"];
          nwr(around:{R},{lat},{lon})["man_made"="works"]["plant"="wastewater"];
          nwr(around:{R},{lat},{lon})["amenity"="wastewater_plant"];
        );
        out center;
        """
        js = _overpass(q, timeout_s=OSM_SNAP_TIMEOUT_S+5)
        if not js or "elements" not in js or len(js["elements"]) == 0:
            continue
        def el_xy(e):
            if "center" in e: return e["center"]["lon"], e["center"]["lat"]
            if e.get("type") == "node" and "lon" in e and "lat" in e: return e["lon"], e["lat"]
            return None
        pts = []
        for e in js["elements"]:
            xy0 = el_xy(e)
            if not xy0: continue
            dx = (xy0[0]-lon)*math.cos(math.radians(lat)); dy = (xy0[1]-lat)
            d2 = dx*dx + dy*dy
            pts.append((d2, xy0))
        if pts:
            pts.sort(key=lambda t: t[0])
            return pts[0][1]
    return None, None

def osm_roads_buildings_in_bbox(bounds, src_crs):
    s, w, n, e = _bbox_to_wgs84(bounds, src_crs)
    q = f"""
    [out:json][timeout:{OSM_OVERLAYS_TIMEOUT_S}];
    (
      way["highway"]["area"!="yes"]({s},{w},{n},{e});
    )->.roads;
    (
      way["building"]({s},{w},{n},{e});
    )->.buildings;
    .roads out geom;
    .buildings out geom;
    """
    js = _overpass(q, timeout_s=OSM_OVERLAYS_TIMEOUT_S+5)
    if not js or "elements" not in js:
        return gpd.GeoDataFrame(geometry=[], crs=src_crs), gpd.GeoDataFrame(geometry=[], crs=src_crs)

    roads = []
    buildings = []
    for el in js["elements"]:
        if "geometry" not in el or len(el["geometry"]) < 2:
            continue
        coords = [(p["lon"], p["lat"]) for p in el["geometry"]]
        if el.get("type") != "way":
            continue
        if "tags" in el and "building" in el["tags"]:
            if coords[0] == coords[-1] and len(coords) >= 4:
                poly = Polygon(coords)
                buildings.append(poly)
        elif "tags" in el and "highway" in el["tags"]:
            line = LineString(coords)
            roads.append(line)

    gdf_roads = gpd.GeoDataFrame(geometry=[*roads], crs=4326).to_crs(src_crs) if roads else gpd.GeoDataFrame(geometry=[], crs=src_crs)
    gdf_blds  = gpd.GeoDataFrame(geometry=[*buildings], crs=4326).to_crs(src_crs) if buildings else gpd.GeoDataFrame(geometry=[], crs=src_crs)
    return gdf_roads, gdf_blds



# =============================================================================
# Excel Update Helpers (Sheet: H2 Logistics)
# =============================================================================
def insert_after(df: pd.DataFrame, after_col: str, new_col: str, values: pd.Series) -> pd.DataFrame:
    out = df.copy()
    out[new_col] = values
    if after_col in out.columns:
        cols = list(out.columns)
        cols.remove(new_col)
        pos = cols.index(after_col) + 1
        cols = cols[:pos] + [new_col] + cols[pos:]
        out = out[cols]
    return out

def place_metric_columns(df: pd.DataFrame, built_cols: list[str],
                         price_cols: list[str], density_cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in price_cols + density_cols:
        if col not in out.columns:
            out[col] = np.nan
    last_idx = max([out.columns.get_loc(c) for c in built_cols if c in out.columns],
                   default=len(out.columns)-1)
    left = list(out.columns[:last_idx+1])
    rest = [c for c in out.columns if c not in left + price_cols + density_cols]
    out = out[left + price_cols + density_cols + rest]
    return out

def update_pipelines_sheet(base_df: pd.DataFrame, key_col: str,
                           scen: str, direct_col: str, built_cols: list[str],
                           routes_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    df = base_df.copy()

    length_km_by_id = (pd.Series(routes_gdf["length_m"].values,
                                 index=routes_gdf["anlage_id"].values) / 1000.0)
    price_by_id = pd.Series(routes_gdf["total_cost"].values,
                            index=routes_gdf["anlage_id"].values)
    dens_by_id  = pd.Series(routes_gdf["cost_density_per_km"].values,
                            index=routes_gdf["anlage_id"].values)
    cross_by_id = pd.Series(routes_gdf["crosses_sensitive"].values,
                            index=routes_gdf["anlage_id"].values)
    b5_by_id    = pd.Series(routes_gdf["built_share_first5km"].values,
                            index=routes_gdf["anlage_id"].values)
    bt_by_id    = pd.Series(routes_gdf["built_share_total"].values,
                            index=routes_gdf["anlage_id"].values)

    least_col   = f"LeastCostLine ({scen}) [km]"
    cross_col   = f"Crosses sensible zone ({scen})"
    price_col   = f"Price ({scen})"
    dens_col    = f"Cost density ({scen}) [per km]"

    df[least_col] = df[key_col].map(length_km_by_id)
    df[cross_col] = df[key_col].map(cross_by_id)

    if direct_col:
        df = insert_after(df, direct_col, least_col, df[least_col])
        df = insert_after(df, least_col, cross_col, df[cross_col])

    df[price_col] = df[key_col].map(price_by_id)
    df[dens_col]  = df[key_col].map(dens_by_id)

    price_cols_all = [f"Price (EHB)"]
    dens_cols_all  = [f"Cost density (EHB) [per km]"]
    df = place_metric_columns(df, [c for c in built_cols if c],
                              price_cols_all, dens_cols_all)
    return df

def write_sheet(sheet_name: str, df: pd.DataFrame):
    with pd.ExcelWriter(WWTP_XLSX, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

# =============================================================================
# Daten laden (General Data + Pipelines)
# =============================================================================
def load_wwtps(raster_crs):
    general = pd.read_excel(WWTP_XLSX, sheet_name="General Data")
    hydro   = pd.read_excel(WWTP_XLSX, sheet_name="H2 Logistics")

    key = choose_join_key(general.columns, hydro.columns)
    if key is None:
        general["__ix__"] = general.index; hydro["__ix__"] = hydro.index; key = "__ix__"
    df = general.merge(hydro, on=key, how="inner")

    if not {"Latitude","Longitude"}.issubset(general.columns):
        raise RuntimeError("Spalten 'Latitude' & 'Longitude' fehlen im Sheet 'General Data'.")

    df  = df.dropna(subset=["Latitude","Longitude"])
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["Longitude"].astype(float), df["Latitude"].astype(float)),
        crs="EPSG:4326"
    ).to_crs(raster_crs)

    for cand in ["UWWTD Code","WWTP_ID","Plant_ID","ID","Id","id","Name","name",key]:
        if cand in gdf.columns: gdf["anlage_id"] = gdf[cand]; break
    else:
        gdf["anlage_id"] = gdf.index

    c1 = find_built_col(gdf.columns, 1)  # EHB
    c2 = find_built_col(gdf.columns, 2)  # Gas

    d_ehb = find_distance_col(gdf.columns, "EHB")
    d_gas = find_distance_col(gdf.columns, "Gas")

    return gdf, hydro, key, c1, c2, d_ehb, d_gas

def load_targets(scenario, raster_crs, wwtps_gdf=None):
    # Gas-Typ Filter automatisch je nach Szenario setzen
    if OSM_GAS_TYPE_FILTER_AUTO:

        gas_filter = "hydrogen"
    else:
        gas_filter = "hydrogen"  # Fallback
    
    if scenario == "EHB":
        frames_to_combine = []
        
        # Lokale EHB-Dateien laden
        files = resolve_ehb_files()
        if files:
            frames = [gpd.read_file(p) for p in files]
            log("   EHB-Dateien: " + " + ".join(p.name for p in files))
            local_gdf = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)
            frames_to_combine.append(local_gdf)
        else:
            log("⚠️  Keine lokalen EHB-Dateien in 'Daten/' gefunden.")
        
        # Kombinieren
        if not frames_to_combine:
            log("⚠️  Keine EHB/Wasserstoff-Pipelines verfügbar")
            return gpd.GeoDataFrame(geometry=[], crs=raster_crs)
        
        if len(frames_to_combine) == 1:
            gdf = frames_to_combine[0]
        else:
            gdf = gpd.GeoDataFrame(
                pd.concat(frames_to_combine, ignore_index=True),
                crs=raster_crs
            )
            log(f"   ✓ Kombiniert: {len(gdf)} Wasserstoff-Pipeline-Segmente gesamt")
    
    else:
        raise ValueError(f"Only EHB scenario supported, got: {scenario}")
    
    if getattr(gdf, "crs", None) is None:
        gdf = gdf.set_crs(raster_crs)
    elif gdf.crs != raster_crs:
        gdf = gdf.to_crs(raster_crs)
    return gdf

# =============================================================================
# Slope-Helper
# =============================================================================
def _add_slope_cost(cost, arr, win, src_base, dem_path):
    if not USE_SLOPE:
        return
    try:
        out_h, out_w = win.height, win.width
        win_transform = rasterio.windows.transform(win, src_base.transform)

        dem_ma = None
        if dem_path and Path(dem_path).exists():
            with rasterio.open(dem_path) as dem_src:
                with WarpedVRT(
                    dem_src, crs=src_base.crs, transform=win_transform,
                    width=out_w, height=out_h, resampling=rasterio.enums.Resampling.bilinear
                ) as vrt:
                    dem_ma = vrt.read(1, out_shape=(out_h, out_w), masked=True)
        if dem_ma is None:
            bounds = (win_transform.c, win_transform.f,
                      win_transform.c + win_transform.a*out_w,
                      win_transform.f + win_transform.e*out_h)
            dem_ma = _wcs_get_dem_window(bounds, out_w, out_h, int(src_base.crs.to_epsg() or 3035))

        if dem_ma is None or getattr(dem_ma, "mask", None) is None or dem_ma.mask.all():
            return

        px = abs(src_base.res[0]); py = abs(src_base.res[1])
        dz_dy, dz_dx = np.gradient(dem_ma.filled(np.nan).astype("float32"), py, px)
        slope_rad = np.arctan(np.hypot(dz_dx, dz_dy))
        slope_deg = np.degrees(slope_rad)
        if SLOPE_MAX_DEG is not None:
            slope_deg = np.clip(slope_deg, 0, float(SLOPE_MAX_DEG))
        delta = SLOPE_COST_PER_DEG * slope_deg

        mask_valid = np.isfinite(cost)
        if SLOPE_IGNORE_WATER:
            mask_valid &= (arr != 10)

        delta = np.where(np.isfinite(delta), delta, 0.0)
        cost[mask_valid] = cost[mask_valid] + delta[mask_valid]

    except Exception as e:
        log(f"⚠️  Slope ignoriert: {e}")

# =============================================================================
# Routing: Protected Areas + OSM-Overlays
# =============================================================================
def _apply_protected_area_costs(cost, arr, win_transform, bounds, src):
    mask_pa_any = None
    if not USE_PROTECTED_AREAS:
        return mask_pa_any
    try:
        out_epsg = int(src.crs.to_epsg() or 3035)
        shapes_by_class, all_shapes = _protected_classes_for_window(bounds, out_epsg)
        if all_shapes:
            mask_pa_any = features.rasterize(
                shapes=[(g, 1) for g in all_shapes],
                out_shape=cost.shape, transform=win_transform,
                fill=0, all_touched=True, dtype="uint8"
            ) == 1
        for cls, geoms in shapes_by_class.items():
            if not geoms:
                continue
            abs_cost = float(PA_CLASS_ABS_COSTS.get(cls, PA_CLASS_ABS_COSTS.get("DEFAULT", None)))
            if abs_cost is None:
                continue
            m = features.rasterize(
                    shapes=[(g, 1) for g in geoms],
                    out_shape=cost.shape, transform=win_transform,
                    fill=0, all_touched=True, dtype="uint8"
                ) == 1
            if not PA_APPLY_TO_WATER:
                m = m & (arr != 10)
            sel = m & np.isfinite(cost)
            if np.any(sel):
                cost[sel] = abs_cost
    except Exception as e:
        log(f"⚠️  Schutzgebiets-Preise ignoriert: {e}")
    return mask_pa_any

def _apply_osm_overlays(cost, arr, bounds, win_transform, src, start_pt: Point, allow_buildings: bool = None):
    """
    allow_buildings: None = nutze BUILDING_IMPASSABLE, True = Gebäude erlauben, False = Gebäude blockieren
    """
    if not ENABLE_OSM_OVERLAYS:
        return
    if _POOL_SCENARIO is not None and not OSM_OVERLAYS_IN_PARALLEL:
        return
    
    # Entscheidung, ob Gebäude blockieren sollen
    building_impassable = BUILDING_IMPASSABLE if allow_buildings is None else (not allow_buildings)
    
    try:
        roads, blds = osm_roads_buildings_in_bbox(bounds, src.crs)

        if not roads.empty:
            roads_buf = roads.copy()
            roads_buf["geometry"] = roads_buf.geometry.buffer(ROAD_BUFFER_M, cap_style=2)
            rmask = features.rasterize(
                shapes=[(g, 1) for g in roads_buf.geometry if g and not g.is_empty],
                out_shape=cost.shape, transform=win_transform,
                fill=0, all_touched=True, dtype="uint8"
            ) == 1
            if not ROAD_APPLY_ON_WATER:
                rmask = rmask & (arr != 10)
            sel = rmask & np.isfinite(cost)
            if np.any(sel):
                cost[sel] = float(ROAD_ABS_COST)
            # Feedback
            try:
                roads_cells = int(sel.sum())
            except Exception:
                roads_cells = 0
            if roads_cells > 0:
                pass
            else:
                pass

        if building_impassable and not blds.empty:
            bmask = features.rasterize(
                shapes=[(g, 1) for g in blds.geometry if g and not g.is_empty],
                out_shape=cost.shape, transform=win_transform,
                fill=0, all_touched=True, dtype="uint8"
            ) == 1
            start_zone_geom = start_pt.buffer(float(START_ALLOW_BUILDING_M))
            start_zone = features.rasterize(
                shapes=[(start_zone_geom, 1)],
                out_shape=cost.shape, transform=win_transform,
                fill=0, all_touched=True, dtype="uint8"
            ) == 1
            impass = bmask & (~start_zone)
            if np.any(impass):
                cost[impass] = np.inf
                # Feedback
                try:
                    b_cells = int(impass.sum())
                except Exception:
                    b_cells = 0
                if b_cells > 0:
                    pass
                else:
                    pass

    except Exception as e:
        log(f"⚠️  OSM-Overlays ignoriert: {e}")

# ---------- Built-Share Berechnung (auf Basis CLC-Array) ----------------------
def _built_shares_for_line(arr, win_transform, geom: LineString) -> tuple[float,float]:
    """Gibt (built_share_total, built_share_first5km) im Bereich 0..1 zurück."""
    if geom is None or geom.is_empty:
        return (np.nan, np.nan)

    # Rastermasken
    def _share_for(gline: LineString):
        mask = features.rasterize(
            shapes=[(gline, 1)],
            out_shape=arr.shape, transform=win_transform,
            fill=0, all_touched=True, dtype="uint8"
        ) == 1
        total = int(mask.sum())
        if total == 0:
            return np.nan
        built = int((mask & (arr == 1)).sum())  # Klasse 1 = Sealed
        return built / total

    share_total = _share_for(geom)
    first5 = _line_first_km(geom, 5.0)
    share_5km = _share_for(first5)

    return (float(share_total) if share_total==share_total else np.nan,
            float(share_5km)   if share_5km==share_5km else np.nan)

# =============================================================================
# Routing
# =============================================================================
def attempt_route(pt: Point, targets_gdf: gpd.GeoDataFrame, src, buffer_m, water_override, allow_buildings=None):
    tgt_hint = nearest_point_on_targets(pt, targets_gdf)

    minx = min(pt.x, tgt_hint.x) - buffer_m; maxx = max(pt.x, tgt_hint.x) + buffer_m
    miny = min(pt.y, tgt_hint.y) - buffer_m; maxy = max(pt.y, tgt_hint.y) + buffer_m
    bounds = (minx, miny, maxx, maxy)

    lut = build_cost_lut(src.nodata, water_override=water_override)
    cost, win, arr = read_cost_window(src, bounds, lut)
    win_transform = rasterio.windows.transform(win, src.transform)

    if SEAL_WATER_GAPS:
        water_raw = (arr == 10)
        water_closed = binary_closing(water_raw, SQUARE_FOOTPRINT(WATER_CLOSE_SIZE))
        water_closed = remove_small_holes(water_closed, area_threshold=WATER_HOLE_AREA)
        water_cost = float(WATER_COST_DEFAULT if water_override is None else water_override)
        cost[water_closed] = water_cost

    _add_slope_cost(cost, arr, win, src, DEM_PATH)

    mask_pa_any = _apply_protected_area_costs(cost, arr, win_transform, bounds, src)
    _apply_osm_overlays(cost, arr, bounds, win_transform, src, start_pt=pt, allow_buildings=allow_buildings)

    tg = gpd.clip(targets_gdf, box(*bounds))
    shapes = [(geom, 1) for geom in tg.geometry if geom is not None and not geom.is_empty]
    mask = features.rasterize(
        shapes=shapes, out_shape=cost.shape, transform=win_transform,
        fill=0, all_touched=True, dtype="uint8"
    )

    r0, c0 = rc_from_xy(src.transform, pt.x, pt.y)
    r0 -= int(win.row_off); c0 -= int(win.col_off)

    inside = lambda sh, r, c: (0 <= r < sh[0]) and (0 <= c < sh[1])
    if not inside(cost.shape, r0, c0):
        return None, "Startpunkt außerhalb des Kostenfensters"
    if not np.isfinite(cost[r0, c0]):
        return None, "Startpunkt hat unendliche Kosten (z.B. Nodata, Schnee/Eis)"

    def _crosses_sensitive_from_path(path_rc) -> int:
        if mask_pa_any is None or path_rc is None or len(path_rc) == 0:
            return 0
        rr = np.array([rc[0] for rc in path_rc], dtype=int)
        cc = np.array([rc[1] for rc in path_rc], dtype=int)
        rr_ok = (rr >= 0) & (rr < mask_pa_any.shape[0])
        cc_ok = (cc >= 0) & (cc < mask_pa_any.shape[1])
        ok = rr_ok & cc_ok
        if not np.any(ok): return 0
        return 1 if np.any(mask_pa_any[rr[ok], cc[ok]]) else 0

    if inside(cost.shape, r0, c0) and mask[r0, c0] == 1:
        eps = 0.01
        line = LineString([(pt.x, pt.y), (pt.x + eps, pt.y)])
        b_tot, b_5 = _built_shares_for_line(arr, win_transform, line)
        return (line, 0.0, "mcp", 0, b_tot, b_5), None

    try:
        mcp = MCP_Geometric(cost)
        costs_arr, _ = mcp.find_costs(starts=[(int(r0), int(c0))])
    except Exception as e:
        costs_arr = None
        mcp_error = f"MCP-Berechnung fehlgeschlagen: {str(e)}"

    if costs_arr is not None:
        end_rc = np.column_stack(np.where(mask == 1))
        if end_rc.size > 0:
            end_costs = costs_arr[end_rc[:, 0], end_rc[:, 1]]
            valid = np.isfinite(end_costs)
            if np.any(valid):
                best_idx = np.where(valid)[0][np.argmin(end_costs[valid])]
                dest_r, dest_c = int(end_rc[best_idx, 0]), int(end_rc[best_idx, 1])
                path = mcp.traceback((dest_r, dest_c))
                if path is not None and len(path) >= 1:
                    line = line_from_path(src.transform, win, path, pt=pt, tgt=None)
                    b_tot, b_5 = _built_shares_for_line(arr, win_transform, line)
                    return (line, float(end_costs[best_idx]), "mcp", _crosses_sensitive_from_path(path), b_tot, b_5), None
            else:
                mcp_error = "Keine erreichbaren Ziele (alle Kosten unendlich)"
        else:
            mcp_error = "Keine Ziel-Pixel im Routing-Fenster gefunden"
    
    # Fallback versuchen
    r1, c1 = rc_from_xy(src.transform, tgt_hint.x, tgt_hint.y)
    r1 -= int(win.row_off); c1 -= int(win.col_off)
    if not inside(cost.shape, r1, c1):
        return None, f"MCP fehlgeschlagen ({mcp_error if 'mcp_error' in locals() else 'unbekannt'}); Fallback: Zielpunkt außerhalb des Fensters"
    if not np.isfinite(cost[r1, c1]):
        return None, f"MCP fehlgeschlagen ({mcp_error if 'mcp_error' in locals() else 'unbekannt'}); Fallback: Zielpunkt hat unendliche Kosten"
    
    try:
        path, total = route_through_array(cost, (r0, c0), (r1, c1),
                                          fully_connected=True, geometric=True)
    except Exception as e:
        return None, f"MCP fehlgeschlagen ({mcp_error if 'mcp_error' in locals() else 'unbekannt'}); Fallback fehlgeschlagen: {str(e)}"
    
    line = line_from_path(src.transform, win, path, pt=pt, tgt=tgt_hint)
    b_tot, b_5 = _built_shares_for_line(arr, win_transform, line)
    return (line, float(total), "fallback", _crosses_sensitive_from_path(path), b_tot, b_5), None

# -------- Worker-Wrapper ----------
def _route_one_worker(args):
    anlage_id, x, y = args
    pt = Point(float(x), float(y))
    attempts = [
        (BUFFER_M,    None, None, "Standard-Buffer"),
        (BUFFER_M*3,  None, None, "3x Buffer"),
        (BUFFER_M*3,  100.0, None, "3x Buffer + Wasser-Override"),
        (BUFFER_M*5,  100.0, None, "5x Buffer + Wasser-Override"),
        (BUFFER_M*5,  100.0, True, "5x Buffer + Wasser-Override + Gebäude erlaubt"),  # Neuer Fallback!
    ]
    line = None; total_cost = None; method = None; cross = 0; btot = np.nan; b5 = np.nan
    failure_reasons = []
    
    for buf, wovr, allow_bld, attempt_desc in attempts:
        res, error = attempt_route(pt, _POOL_TARGETS, _POOL_SRC, buf, wovr, allow_buildings=allow_bld)
        if res is not None:
            line, total_cost, method, cross, btot, b5 = res
            break
        if error:
            failure_reasons.append(f"{attempt_desc}: {error}")
    
    if line is None:
        combined_reason = " | ".join(failure_reasons) if failure_reasons else "Alle Routing-Versuche fehlgeschlagen"
        return None, combined_reason
    return (anlage_id, float(total_cost), float(line.length), method, int(cross), float(btot), float(b5), line.wkb), None

# -------- Sequentiell ----------
def route_for_points(points_gdf, targets_gdf, src, scenario_name):
    results, totals, lengths, ids, methods, crosses, btotals, bfirst5 = [], [], [], [], [], [], [], []
    failures = {}  # Dict: anlage_id -> Fehlergrund
    total_n = len(points_gdf)
    proc = 0; ok = ok_mcp = ok_fb = 0
    progress_bar(proc, total_n, ok, ok_mcp, ok_fb)
    for _, row in points_gdf.iterrows():
        pt  = row.geometry if isinstance(row.geometry, Point) else row.geometry.centroid
        attempts = [
            (BUFFER_M,    None, None, "Standard-Buffer"),
            (BUFFER_M*3,  None, None, "3x Buffer"),
            (BUFFER_M*3,  100.0, None, "3x Buffer + Wasser-Override"),
            (BUFFER_M*5,  100.0, None, "5x Buffer + Wasser-Override"),
            (BUFFER_M*5,  100.0, True, "5x Buffer + Wasser-Override + Gebäude erlaubt"),  # Neuer Fallback!
        ]
        line = None; total_cost = None; method = None; cross = 0; btot=np.nan; b5=np.nan
        failure_reasons = []
        
        for buf, wovr, allow_bld, attempt_desc in attempts:
            res, error = attempt_route(pt, targets_gdf, src, buf, wovr, allow_buildings=allow_bld)
            if res is not None:
                line, total_cost, method, cross, btot, b5 = res
                break
            if error:
                failure_reasons.append(f"{attempt_desc}: {error}")
        
        proc += 1
        if line is not None:
            ok += 1
            if method == "mcp": ok_mcp += 1
            else: ok_fb += 1
            results.append(line); totals.append(float(total_cost)); lengths.append(float(line.length))
            ids.append(row["anlage_id"]); methods.append(method); crosses.append(int(cross))
            btotals.append(float(btot)); bfirst5.append(float(b5))
        else:
            combined_reason = " | ".join(failure_reasons) if failure_reasons else "Alle Routing-Versuche fehlgeschlagen"
            failures[row["anlage_id"]] = combined_reason
        progress_bar(proc, total_n, ok, ok_mcp, ok_fb)
    progress_bar(proc, total_n, ok, ok_mcp, ok_fb, end=True)
    length_km = np.array(lengths, dtype="float64") / 1000.0
    with np.errstate(divide="ignore", invalid="ignore"):
        density = np.where(length_km > 0, np.array(totals) / length_km, 0.0)
    gdf = gpd.GeoDataFrame(
        {"anlage_id": ids, "scenario": scenario_name, "method": methods,
         "total_cost": totals, "length_m": lengths, "crosses_sensitive": crosses,
         "built_share_total": btotals, "built_share_first5km": bfirst5,
         "cost_density_per_km": density,
         "geometry": results},
        crs=src.crs
    )
    return gdf, failures

# -------- Parallel ----------
def route_for_points_parallel(points_gdf, scenario_name, plants_crs):
    ids, totals, lengths, methods, crosses, btotals, bfirst5, geoms = [], [], [], [], [], [], [], []
    failures = {}  # Dict: anlage_id -> Fehlergrund
    tasks = [(row["anlage_id"], float(row.geometry.x), float(row.geometry.y))
             for _, row in points_gdf.iterrows()]
    total_n = len(tasks)
    proc = 0; ok = ok_mcp = ok_fb = 0
    progress_bar(proc, total_n, ok, ok_mcp, ok_fb)

    with ProcessPoolExecutor(max_workers=MAX_WORKERS,
                             initializer=_pool_init,
                             initargs=(scenario_name, str(RASTER), points_gdf)) as ex:
        futures = {ex.submit(_route_one_worker, t): t[0] for t in tasks}
        for fut in as_completed(futures):
            anlage_id = futures[fut]
            try:
                res, error = fut.result()
            except Exception as e:
                res = None
                error = f"Worker-Exception: {str(e)}"
            proc += 1
            if res is None:
                failures[anlage_id] = error if error else "Unbekannter Fehler"
            else:
                aid, total_cost, length_m, method, cross, btot, b5, wkb = res
                ids.append(aid); totals.append(total_cost); lengths.append(length_m)
                methods.append(method); crosses.append(cross)
                btotals.append(btot); bfirst5.append(b5)
                geoms.append(_wkb.loads(wkb))
                if method == "mcp": ok_mcp += 1
                else: ok_fb += 1
            ok = len(ids)
            progress_bar(proc, total_n, ok, ok_mcp, ok_fb)

    progress_bar(proc, total_n, len(ids), ok_mcp, ok_fb, end=True)

    length_km = np.array(lengths, dtype="float64") / 1000.0
    with np.errstate(divide="ignore", invalid="ignore"):
        density = np.where(length_km > 0, np.array(totals) / length_km, 0.0)
    gdf = gpd.GeoDataFrame(
        {"anlage_id": ids, "scenario": scenario_name, "method": methods,
         "total_cost": totals, "length_m": lengths, "crosses_sensitive": crosses,
         "built_share_total": btotals, "built_share_first5km": bfirst5,
         "cost_density_per_km": density,
         "geometry": geoms},
        crs=plants_crs
    )
    return gdf, failures

# =============================================================================
# OSM-Snapping der Startpunkte
# =============================================================================
def apply_osm_snapping(plants_gdf_w3035: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if not ENABLE_OSM_SNAPPING:
        return plants_gdf_w3035
    log("\n[OSM] Snapping auf nächste Kläranlagen-Geometrie …")
    snapped_x = []; snapped_y = []; used = []
    total = len(plants_gdf_w3035)
    proc = 0; ok = 0
    def pbar(proc, ok, total, width=32, end=False):
        pct = 0.0 if total == 0 else proc/total
        filled = int(width * pct)
        bar = "█"*filled + "─"*(width - filled)
        print(f"\r[SNAP {bar}] {pct*100:5.1f}%  {proc}/{total}  OK:{ok}", end=("" if not end else "\n"), flush=True)
    pbar(proc, ok, total)
    for _, row in plants_gdf_w3035.to_crs(4326).iterrows():
        lon = float(row.geometry.x); lat = float(row.geometry.y)
        slon, slat = osm_snap_wwtp(lon, lat)
        if slon is None or slat is None:
            snapped_x.append(np.nan); snapped_y.append(np.nan); used.append(0)
        else:
            p = gpd.GeoSeries([Point(slon, slat)], crs=4326).to_crs(plants_gdf_w3035.crs).iloc[0]
            snapped_x.append(p.x); snapped_y.append(p.y); used.append(1); ok += 1
        proc += 1; pbar(proc, ok, total)
    pbar(proc, ok, total, end=True)
    out = plants_gdf_w3035.copy()
    out["snap_x"] = snapped_x; out["snap_y"] = snapped_y; out["snap_used"] = used
    has = out["snap_used"] == 1
    out.loc[has, "geometry"] = gpd.points_from_xy(out.loc[has, "snap_x"], out.loc[has, "snap_y"], crs=out.crs)
    log(f"[OSM] Snapped: {int(has.sum())}/{len(out)}")
    return out

# =============================================================================
# EXISTING ROUTES: Laden & Kennzahlen sichern
# =============================================================================
def routes_file_path_for_scenario(scen_name: str) -> Path:
    return OUT_DIR / f"routes_{scen_name.lower()}.gpkg"

def _ensure_metrics_for_routes(routes: gpd.GeoDataFrame, src) -> gpd.GeoDataFrame:
    if routes.empty:
        return routes
    # CRS schon korrekt gesetzt?
    if getattr(routes, "crs", None) != src.crs:
        routes = routes.to_crs(src.crs)

    # Länge
    if "length_m" not in routes.columns or not pd.api.types.is_numeric_dtype(routes["length_m"]):
        routes["length_m"] = routes.length.astype("float64")

    # Crosses sensitive
    if "crosses_sensitive" not in routes.columns:
        minx, miny, maxx, maxy = routes.total_bounds
        bounds = (minx, miny, maxx, maxy)
        _, all_shapes = _protected_classes_for_window(bounds, int(src.crs.to_epsg() or 3035))
        if all_shapes:
            pa_union = unary_union(all_shapes)
            routes["crosses_sensitive"] = routes.geometry.apply(lambda g: int(g is not None and not g.is_empty and g.intersects(pa_union)))
        else:
            routes["crosses_sensitive"] = 0

    # Built shares
    need_b = ("built_share_total" not in routes.columns) or ("built_share_first5km" not in routes.columns)
    if need_b:
        shares_total = []
        shares_5 = []
        for geom in routes.geometry:
            if geom is None or geom.is_empty:
                shares_total.append(np.nan); shares_5.append(np.nan); continue
            minx, miny, maxx, maxy = geom.bounds
            pad = 30.0
            bounds = (minx-pad, miny-pad, maxx+pad, maxy+pad)
            lut = build_cost_lut(src.nodata, water_override=None)
            cost, win, arr = read_cost_window(src, bounds, lut)
            win_transform = rasterio.windows.transform(win, src.transform)
            s_tot, s_5 = _built_shares_for_line(arr, win_transform, geom)
            shares_total.append(s_tot); shares_5.append(s_5)
        routes["built_share_total"]   = shares_total
        routes["built_share_first5km"]= shares_5

    # total_cost + cost_density
    if "total_cost" not in routes.columns or routes["total_cost"].isna().any():
        totals = []
        for geom in routes.geometry:
            if geom is None or geom.is_empty:
                totals.append(np.nan); continue
            minx, miny, maxx, maxy = geom.bounds
            pad = 30.0
            bounds = (minx-pad, miny-pad, maxx+pad, maxy+pad)
            lut = build_cost_lut(src.nodata, water_override=None)
            cost, win, arr = read_cost_window(src, bounds, lut)
            win_transform = rasterio.windows.transform(win, src.transform)
            if SEAL_WATER_GAPS:
                water_raw = (arr == 10)
                water_closed = binary_closing(water_raw, SQUARE_FOOTPRINT(WATER_CLOSE_SIZE))
                water_closed = remove_small_holes(water_closed, area_threshold=WATER_HOLE_AREA)
                cost[water_closed] = float(WATER_COST_DEFAULT)
            _add_slope_cost(cost, arr, win, src, DEM_PATH)
            _apply_protected_area_costs(cost, arr, win_transform, bounds, src)
            try:
                start_pt = Point(*list(geom.coords)[0])
            except Exception:
                start_pt = geom.representative_point()
            _apply_osm_overlays(cost, arr, bounds, win_transform, src, start_pt=start_pt)
            mask = features.rasterize(
                shapes=[(geom, 1)],
                out_shape=cost.shape, transform=win_transform,
                fill=0, all_touched=True, dtype="uint8"
            ) == 1
            sel = mask & np.isfinite(cost)
            total = float(np.nansum(cost[sel])) if np.any(sel) else np.nan
            totals.append(total)
        routes["total_cost"] = totals

    length_km = (routes["length_m"].astype("float64") / 1000.0).replace(0, np.nan)
    routes["cost_density_per_km"] = routes["total_cost"] / length_km

    if "method" not in routes.columns:
        routes["method"] = "existing"
    return routes

def load_existing_routes_if_any(scen_name: str, src_crs) -> gpd.GeoDataFrame:
    gpkg = routes_file_path_for_scenario(scen_name)
    if not gpkg.exists():
        return gpd.GeoDataFrame(geometry=[], crs=src_crs)
    try:
        layers = fiona.listlayers(gpkg)
        layer = None
        cand = f"routes_{scen_name.lower()}"
        for ly in layers:
            if ly.lower() == cand:
                layer = ly; break
        if layer is None:
            layer = layers[0]
        gdf = gpd.read_file(gpkg, layer=layer)
    except Exception:
        return gpd.GeoDataFrame(geometry=[], crs=src_crs)

    if getattr(gdf, "crs", None) is None:
        gdf = gdf.set_crs(src_crs)
    elif gdf.crs != src_crs:
        gdf = gdf.to_crs(src_crs)

    if "anlage_id" not in gdf.columns:
        for k in ["UWWTD_Code","UWWTD Code","WWTP_ID","Plant_ID","ID","id","Name","name"]:
            if k in gdf.columns:
                gdf["anlage_id"] = gdf[k]; break
        if "anlage_id" not in gdf.columns:
            gdf["anlage_id"] = np.arange(len(gdf))

    if "length_m" not in gdf.columns or not pd.api.types.is_numeric_dtype(gdf["length_m"]):
        gdf["length_m"] = gdf.length.astype("float64")

    return gdf

# =============================================================================
# Main
# =============================================================================

# =============================================================================
# Startpunkte aus lokalen WWTP-Polygonen (vor OSM-Snapping)
# =============================================================================
def _largest_polygon(geom):
    """Gibt das größte Einzel-Polygon zurück (bei MultiPolygon), sonst None."""
    try:
        if isinstance(geom, MultiPolygon):
            return max(list(geom.geoms), key=lambda g: g.area) if len(geom.geoms) else None
        if isinstance(geom, Polygon):
            return geom
    except Exception:
        pass
    return None

def apply_available_area_startpoints(plants_gdf_w3035: gpd.GeoDataFrame, targets_gdf: gpd.GeoDataFrame = None) -> gpd.GeoDataFrame:
    """
    Ersetzt die Startpunkte durch den Zentroid des Free Area Polygons, das am nächsten
    zu den Pipelines/Industrie liegt.
    Nutzt die Metadaten (Code, Name, Lat, Lon) um die richtigen Polygone zuzuordnen.
    """
    if plants_gdf_w3035 is None or plants_gdf_w3035.empty:
        return plants_gdf_w3035
    try:
        if not FREE_AREA_GPKG.exists():
            log(f"⚠️  WWTP_Free_Area.gpkg nicht gefunden ({FREE_AREA_GPKG}). Überspringe Startpunkt-Anpassung.")
            return plants_gdf_w3035

        # Free Area laden - automatisch ersten Polygon-Layer finden
        layers = fiona.listlayers(FREE_AREA_GPKG)
        poly_layer = None
        for lyr in layers:
            try:
                test_gdf = gpd.read_file(FREE_AREA_GPKG, layer=lyr, rows=slice(0, 1))
                if not test_gdf.empty and test_gdf.geometry.iloc[0] is not None:
                    gt = str(test_gdf.geometry.iloc[0].geom_type).lower()
                    if "polygon" in gt:
                        poly_layer = lyr
                        break
            except Exception:
                continue
        
        if poly_layer is None:
            log("⚠️  WWTP_Free_Area.gpkg hat keinen Polygon-Layer. Überspringe Startpunkt-Anpassung.")
            return plants_gdf_w3035
        
        available = gpd.read_file(FREE_AREA_GPKG, layer=poly_layer)
        if available is None or available.empty:
            log("⚠️  WWTP_Free_Area.gpkg enthält keine Geometrien. Überspringe Startpunkt-Anpassung.")
            return plants_gdf_w3035

        # nur (Multi-)Polygone behalten
        available = available[available.geometry.notna()].copy()
        available = available[available.geometry.geom_type.isin(["Polygon","MultiPolygon"])]
        if available.empty:
            log("⚠️  WWTP_Free_Area.gpkg hat keine (Multi-)Polygon-Geometrien. Überspringe Startpunkt-Anpassung.")
            return plants_gdf_w3035

        # CRS anpassen
        if getattr(available, "crs", None) is None:
            available = available.set_crs(plants_gdf_w3035.crs)
        elif available.crs != plants_gdf_w3035.crs:
            available = available.to_crs(plants_gdf_w3035.crs)

        new_geoms = []
        changed = 0

        for _, row in plants_gdf_w3035.iterrows():
            pt = row.geometry if isinstance(row.geometry, Point) else row.geometry.centroid
            
            # Versuche Zuordnung über anlage_id, Code, Name oder Koordinaten
            anlage_id = row.get("anlage_id")
            matched_polys = None
            
            # 1. Versuch: über Code
            if "Code" in available.columns and anlage_id is not None:
                matched_polys = available[available["Code"].astype(str) == str(anlage_id)]
            
            # 2. Versuch: über Name + Koordinaten-Nähe
            if (matched_polys is None or matched_polys.empty) and "Name" in row and "Name" in available.columns:
                name_match = available[available["Name"] == row.get("Name")]
                if not name_match.empty and "Latitude" in available.columns and "Longitude" in available.columns:
                    # Prüfe Koordinaten-Nähe (innerhalb 100m)
                    pt_wgs = gpd.GeoSeries([pt], crs=plants_gdf_w3035.crs).to_crs(4326).iloc[0]
                    for idx, arow in name_match.iterrows():
                        dist = math.hypot(pt_wgs.x - arow["Longitude"], pt_wgs.y - arow["Latitude"]) * 111000  # grobe Umrechnung
                        if dist < 100:
                            matched_polys = name_match
                            break
            
            # 3. Versuch: nächstgelegenes Polygon im Umkreis (max 1 km)
            if matched_polys is None or matched_polys.empty:
                # Suche im 1 km Radius
                buf = pt.buffer(1000)
                matched_polys = available[available.intersects(buf)]
            
            # Wähle Polygon das am nächsten zum Ziel liegt
            chosen = None
            if matched_polys is not None and not matched_polys.empty:
                if targets_gdf is not None and not targets_gdf.empty:
                    # Finde nächsten Zielpunkt
                    target_union = unary_union(targets_gdf.geometry.values)
                    nearest_target = nearest_points(pt, target_union)[1]
                    
                    # Wähle Polygon dessen Zentroid am nächsten zum Ziel liegt
                    min_dist_to_target = float('inf')
                    for _, apoly in matched_polys.iterrows():
                        geom = apoly.geometry
                        lg = _largest_polygon(geom)
                        if lg is not None:
                            centroid = lg.centroid
                            dist_to_target = centroid.distance(nearest_target)
                            if dist_to_target < min_dist_to_target:
                                min_dist_to_target = dist_to_target
                                chosen = lg
                else:
                    # Fallback: nächstgelegenes Polygon zum WWTP
                    min_dist = float('inf')
                    for _, apoly in matched_polys.iterrows():
                        geom = apoly.geometry
                        lg = _largest_polygon(geom)
                        if lg is not None:
                            dist = pt.distance(lg.centroid)
                            if dist < min_dist:
                                min_dist = dist
                                chosen = lg

            if chosen is not None:
                # Verwende den Zentroid des Polygons, das am nächsten zu den Zielen liegt
                new_geoms.append(chosen.centroid)
                changed += 1
            else:
                new_geoms.append(pt)

        out = plants_gdf_w3035.copy()
        out["geometry"] = new_geoms
        log(f"[Startpunkte] WWTP_Free_Area.gpkg verwendet → {changed}/{len(out)} Punkte auf Zentroid des nächstgelegenen Polygons gesetzt.")
        return out

    except Exception as e:
        log(f"⚠️  Startpunkte aus WWTP_Free_Area.gpkg nicht anwendbar: {e}")
        return plants_gdf_w3035

def apply_wwtp_shapes_startpoints(plants_gdf_w3035: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    DEPRECATED: Nutze stattdessen apply_available_area_startpoints()
    Ersetzt die Startpunkte durch den Zentroid des jeweils größten WWTP-Polygons
    in der Nähe (Radius gestaffelt nach OSM_SNAP_RADII_M) aus WWTP_Shapes.gpkg.
    Alles andere bleibt unverändert.
    """
    if plants_gdf_w3035 is None or plants_gdf_w3035.empty:
        return plants_gdf_w3035
    try:
        if not WWTP_SHAPES_GPKG.exists():
            log(f"⚠️  WWTP_Shapes.gpkg nicht gefunden ({WWTP_SHAPES_GPKG}). Überspringe Startpunkt-Anpassung.")
            return plants_gdf_w3035

        # möglichen Polygon-Layer erkennen
        layer = None
        try:
            for ly in fiona.listlayers(WWTP_SHAPES_GPKG):
                gtry = gpd.read_file(WWTP_SHAPES_GPKG, layer=ly, rows=slice(0, 1))
                if not gtry.empty and gtry.geometry.iloc[0] is not None:
                    gt = str(gtry.geometry.iloc[0].geom_type).lower()
                    if "polygon" in gt:
                        layer = ly
                        break
        except Exception:
            layer = None

        shapes = gpd.read_file(WWTP_SHAPES_GPKG, layer=layer) if layer else gpd.read_file(WWTP_SHAPES_GPKG)
        if shapes is None or shapes.empty:
            log("⚠️  WWTP_Shapes.gpkg enthält keine Geometrien. Überspringe Startpunkt-Anpassung.")
            return plants_gdf_w3035

        # nur (Multi-)Polygone behalten
        shapes = shapes[shapes.geometry.notna()].copy()
        shapes = shapes[shapes.geometry.geom_type.isin(["Polygon","MultiPolygon"])]
        if shapes.empty:
            log("⚠️  WWTP_Shapes.gpkg hat keine (Multi-)Polygon-Geometrien. Überspringe Startpunkt-Anpassung.")
            return plants_gdf_w3035

        # CRS anpassen
        if getattr(shapes, "crs", None) is None:
            shapes = shapes.set_crs(plants_gdf_w3035.crs)
        elif shapes.crs != plants_gdf_w3035.crs:
            shapes = shapes.to_crs(plants_gdf_w3035.crs)

        # räumlicher Index (optional)
        try:
            sidx = shapes.sindex
        except Exception:
            sidx = None

        new_geoms = []
        changed = 0

        for _, row in plants_gdf_w3035.iterrows():
            pt = row.geometry if isinstance(row.geometry, Point) else row.geometry.centroid
            chosen = None

            for R in OSM_SNAP_RADII_M:
                try:
                    buf = pt.buffer(float(R))
                except Exception:
                    buf = None
                if buf is None:
                    continue

                cand = None
                if sidx is not None:
                    try:
                        hits = list(sidx.query(buf))
                        if hits:
                            cand = shapes.iloc[hits]
                            cand = cand[cand.intersects(buf)]
                    except Exception:
                        cand = shapes[shapes.intersects(buf)]
                else:
                    cand = shapes[shapes.intersects(buf)]

                if cand is not None and not cand.empty:
                    # größtes Einzel-Polygon bestimmen
                    best_geom = None
                    best_area = -1.0
                    for g in cand.geometry:
                        lg = _largest_polygon(g)
                        if lg is not None:
                            area = float(lg.area)
                            if area > best_area:
                                best_area = area
                                best_geom = lg
                    if best_geom is not None:
                        chosen = best_geom
                        break

            if chosen is not None:
                new_geoms.append(chosen.centroid)
                changed += 1
            else:
                new_geoms.append(pt)

        out = plants_gdf_w3035.copy()
        out["geometry"] = new_geoms
        log(f"[Startpunkte] WWTP_Shapes.gpkg verwendet → {changed}/{len(out)} Punkte auf Polygon-Zentroid gesetzt.")
        return out

    except Exception as e:
        log(f"⚠️  Startpunkte aus WWTP_Shapes.gpkg nicht anwendbar: {e}")
        return plants_gdf_w3035

def main():
    with rasterio.open(RASTER) as src:
        if not (abs(src.res[0]-10) < 1e-6 and abs(src.res[1]-10) < 1e-6):
            raise RuntimeError(f"Rasterauflösung ist {src.res}, erwartet 10 m.")
        log(f"Raster OK. CRS={src.crs}, Auflösung={src.res}, Nodata={src.nodata}")

        plants, pipelines_df, key_col, c1, c2, d_ehb, d_gas = load_wwtps(src.crs)
        log(f"WWTPs geladen: {len(plants)} Punkte.")
        log(f"Built-Columns erkannt: 1='{c1}', 2='{c2}'")

        def _use_existing_or_route(scen_name, built_col, direct_col):
            failures = {}  # Dict: anlage_id -> Fehlergrund
            routes_exist = load_existing_routes_if_any(scen_name, plants.crs) if PREFER_EXISTING_ROUTES else gpd.GeoDataFrame(geometry=[], crs=plants.crs)
            if not routes_exist.empty:
                log(f"[{scen_name}] vorhandenes GPKG gefunden → nutze vorhandene Routen ({len(routes_exist)} Linien).")
                routes_exist = _ensure_metrics_for_routes(routes_exist, src)
                nonlocal pipelines_df
                pipelines_df = update_pipelines_sheet(pipelines_df, key_col, scen_name, direct_col, [c1, c2], routes_exist)
                return failures

            if not built_col:
                return failures
            subset = plants[plants[built_col].fillna(0).astype(int) == 1].copy()
            log(f"\n[{scen_name}] ausgewählte Anlagen: {len(subset)} (Flag='{built_col}')")
            targets = load_targets(scen_name, src.crs, wwtps_gdf=plants)
            log(f"[{scen_name}] Ziel-Features: {len(targets)}")
            if subset.empty or targets.empty:
                return failures
            
            # Startpunkte anpassen: nächster Punkt der Available Area zum Ziel
            subset = apply_available_area_startpoints(subset, targets)

            if ENABLE_PARALLEL and len(subset) >= PARALLEL_THRESHOLD:
                routes, failures = route_for_points_parallel(subset, scen_name, plants_crs=plants.crs)
            else:
                routes, failures = route_for_points(subset, targets, src, scen_name)

            if not routes.empty:
                out_full = routes_file_path_for_scenario(scen_name)
                routes.to_file(out_full, layer=f"routes_{scen_name.lower()}", driver="GPKG")
                log(f"[{scen_name}] → GPKG geschrieben: {out_full.name} ({len(routes)} Linien)")
                pipelines_df = update_pipelines_sheet(pipelines_df, key_col, scen_name, direct_col, [c1, c2], routes)
            return failures

        # EHB
        ehb_fail = _use_existing_or_route("EHB", c1, d_ehb)
        if ehb_fail:
            log(f"\n[EHB] NICHT berechnet ({len(ehb_fail)} Anlagen):")
            for aid, reason in ehb_fail.items():
                log(f"  • {aid}: {reason}")

        # Gas pipelines removed - only EHB supported

        write_sheet("H2 Logistics", pipelines_df)
        log("→ Excel aktualisiert (Sheet 'H2 Logistics').")

    log("\nFertig.")

if __name__ == "__main__":
    main()
