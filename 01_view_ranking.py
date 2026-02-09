from __future__ import annotations

import io
import os
import math
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, MultiPolygon
from PIL import Image, ImageTk, ImageDraw, ImageStat
from pyproj import Transformer
import fiona

import tkinter as tk
from tkinter import ttk, filedialog
import threading

import webbrowser
import socket
import html as _html, json as _json
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading as _threading_for_server
from urllib.parse import urlencode

DEBUG = False  # bei Bedarf auf True setzen


def log(msg: str) -> None:
    """Leichtgewichtiges Debug-Logging (kein logging-Framework)."""
    if DEBUG:
        print(msg)


# ------------------------- Pfade / Anzeige -------------------------

def get_available_ranked_files():
    ranked_folder = Path("Output") / "MCA" / "Ranked"
    if not ranked_folder.exists():
        return {"EHB": str(ranked_folder / "UWWTD_TP_Database_ranked_EHB.xlsx")}

    files = {}
    pattern = "UWWTD_TP_Database_ranked_*.xlsx"

    for file_path in ranked_folder.glob(pattern):
        filename = file_path.stem
        parts = filename.split("_")
        if len(parts) >= 5:
            variant = parts[4]  # EHB, etc.
            if len(parts) > 5:
                run_name = "_".join(parts[5:])
                display_name = f"{variant} ({run_name})"
            else:
                display_name = variant
            files[display_name] = str(file_path)

    if not files:
        files["EHB"] = str(ranked_folder / "UWWTD_TP_Database_ranked_EHB.xlsx")

    return files


XLSX_OPTIONS = get_available_ranked_files()
XLSX_PATH = list(XLSX_OPTIONS.values())[0] if XLSX_OPTIONS else ""
GPKG_PATH = str(Path("Output") / "WWTP Geopackages" / "WWTPS_Shapes.gpkg")

FREE_AREA_GPKG = str(Path("Daten") / "WWTP_Free_Area.gpkg")
PIPELINE_GPKGS = [
    str(Path("Output") / "Geopackages" / "routes_ehb.gpkg"),
]

JSON_HP_PATH = str(Path("Daten") / "High Pressure Distribiution.json")
JSON_TRANSMISSION_PATH = str(Path("Daten") / "Transmission.json")

DISTRICT_HEATING_GEOJSON = str(
    Path("Daten") / "District Heating Geodata" / "opendata-fcu-220725" / "reseaux_de_chaleur.geojson")

DISPLAY_W = 740
DISPLAY_H = 360
SIDEPANEL_W = 360  # feste Breite für die rechte Info-Spalte
MAP_PAD = 8  # Innenabstand um den Kartenausschnitt


def center_on_screen(win, width: int, height: int) -> None:
    try:
        win.update_idletasks()
        sw = win.winfo_screenwidth()
        sh = win.winfo_screenheight()
    except Exception:
        return
    x = max((sw - width) // 2, 0)
    y = max((sh - height) // 2, 0)
    win.geometry(f"{width}x{height}+{x}+{y}")


DEFAULT_HALF_SIZE_M = 1500

# ------------------------- WMS-Quellen -------------------------
EEA_CAPS = "https://image.discomap.eea.europa.eu/arcgis/services/GioLand/VHR_2021_LAEA/ImageServer/WMSServer/?request=GetCapabilities&service=WMS"
EOX_WMS = {
    "url": "https://tiles.maps.eox.at/wms?",
    "version": "1.3.0",
    "layer": "s2cloudless_3857",
    "crs": "EPSG:3857",
    "format": "image/jpeg",
    "provider": "EOX S2 cloudless",
}

RAW_ENDPOINTS = {
    # Bundesländer
    "Berlin": "https://gdi.berlin.de/services/wms/dop_2025_fruehjahr?REQUEST=GetCapabilities&SERVICE=WMS",
    "Bayern": "https://geoservices.bayern.de/od/wms/dop/v1/dop20",
    "Bremen": "https://geodienste.bremen.de/wms_dop_lb",
    "Niedersachsen": "https://opendata.lgln.niedersachsen.de/doorman/noauth/dop_wms",
    # Länder
    "FR": "https://data.geopf.fr/wms-r/wms?VERSION=1.3.0",
    "HR": "https://geoportal.dgu.hr/services/inspire/orthophoto_2022/wms",
    "PL": "https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WMS/HighResolution",
    "ES": "https://www.ign.es/wms-inspire/pnoa-ma?",
    "CH": "https://wms.geo.admin.ch/",
    "HU": "https://inspire.lechnerkozpont.hu/geoserver/OI.2020/wms?VERSION=1.3.0",
    "NL": "https://service.pdok.nl/hwh/luchtfotorgb/wms/v1_0?request=GetCapabilities&service=WMS",
}

# Feste bewährte Layer
FIXED_LAYERS = {
    "Bayern": ("by_dop20c", "1.3.0"),
    "FR": ("HR.ORTHOIMAGERY.ORTHOPHOTOS", "1.3.0"),
    "ES": ("OI.OrthoimageCoverage", "1.1.1"),
    "CH": ("ch.swisstopo.swissimage", "1.3.0"),
    "Niedersachsen": ("DOP20", "1.3.0"),  # exakt dieser Layer
}

# Globale CRS-Präferenz (EPSG:4326 kommt bewusst hinten)
CRS_PREF = [
    "EPSG:3857",
    "EPSG:25833", "EPSG:25832", "EPSG:25831",
    "EPSG:2056",
    "EPSG:3035",
    "EPSG:32633", "EPSG:32632", "EPSG:32631",
    "EPSG:25830",
    "EPSG:2180",  # PL
    "CRS:84",
    "EPSG:4326",
]

# Pro-Provider Tuning (Fenstergröße, Requestgröße, CRS-Prio, Vendor-Parameter)
TUNING: Dict[str, Dict[str, Any]] = {
    "PL": {  # scharf + korrekt zentriert
        "half_size_m": 400,
        "req_w": 2400, "req_h": 1500,
        "prefer_crs": ["EPSG:3857", "EPSG:2180", "CRS:84", "EPSG:4326"],
        "vendor": {"MAP_RESOLUTION": "400", "DPI": "400", "FORMAT_OPTIONS": "dpi:400"},
    },
    "Berlin": {"half_size_m": 800, "req_w": 1200, "req_h": 800},
    "Niedersachsen": {"half_size_m": 900, "req_w": 1200, "req_h": 800},
    "ES": {"half_size_m": 900, "req_w": 1400, "req_h": 900},
    "CH": {"half_size_m": 800, "req_w": 1400, "req_h": 900},
}

# Layer-Filter
POS_KEYS = ["ORTHO", "ORTHOPHOTO", "ORTHOIMAGERY", "DOP", "DOP20",
            "SWISSIMAGE", "PNOA", "ORTHOIMAGECOVERAGE", "LUFTBILD", "RGB"]
NEG_KEYS = ["GRID", "INDEX", "LEGEND", "LEGENDE", "OVERLAY", "COPYRIGHT",
            "RAHMEN", "TILE", "KACHEL", "BOUNDARY", "UMRING",
            "HATCH", "SCHRAFF", "UEBERSICHT", "ÜBERSICHT", "MASK", "MASKEN",
            "INFO "]  # unterdrückt z. B. "DOP20 Info"


# ------------------------- Helpers for Esri Map Viewer -------------------------
def _gpkg_to_geojson_str(gpkg_path: str):
    try:
        import geopandas as gpd, fiona, pandas as pd, os
    except Exception:
        return None
    if not gpkg_path or not os.path.exists(gpkg_path):
        return None
    try:
        layers = []
        for lyr in fiona.listlayers(gpkg_path):
            try:
                gdf = gpd.read_file(gpkg_path, layer=lyr)
                if gdf is None or gdf.empty:
                    continue
                if gdf.crs is None:
                    gdf = gdf.set_crs("EPSG:4326", allow_override=True)
                else:
                    gdf = gdf.to_crs("EPSG:4326")
                gdf = gdf[~gdf.geometry.isna() & ~gdf.geometry.is_empty]
                if not gdf.empty:
                    layers.append(gdf)
            except Exception:
                continue
        if not layers:
            return None
        import pandas as pd
        all_gdf = pd.concat(layers, ignore_index=True)
        if all_gdf.empty:
            return None
        try:
            return all_gdf.to_json(drop_id=True)
        except TypeError:
            return all_gdf.to_json()
    except Exception:
        return None


def _buffer_point(lon, lat, radius_m):
    import geopandas as gpd
    from shapely.geometry import Point
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326")
    return pt.to_crs(3857).buffer(radius_m).to_crs(4326).iloc[0]


def _gpkg_polygons_within_radius(gpkg_path: str, lon: float, lat: float, radius_m: float):
    try:
        import geopandas as gpd, fiona, pandas as pd, os
    except Exception:
        return None
    if not gpkg_path or not os.path.exists(gpkg_path):
        return None
    try:
        buf = _buffer_point(lon, lat, radius_m)
        gdfs = []
        for lyr in fiona.listlayers(gpkg_path):
            try:
                gdf = gpd.read_file(gpkg_path, layer=lyr)
                if gdf.empty:
                    continue
                gt = gdf.geometry.iloc[0].geom_type.lower()
                if "polygon" not in gt:
                    continue
                gdf = gdf.set_crs("EPSG:4326", allow_override=True) if gdf.crs is None else gdf.to_crs("EPSG:4326")
                gdf = gdf[gdf.intersects(buf)]
                if not gdf.empty:
                    gdfs.append(gdf)
            except Exception:
                continue
        if not gdfs:
            return None
        import pandas as pd
        out = pd.concat(gdfs, ignore_index=True)
        return out.to_json()
    except Exception:
        return None


def _gpkg_lines_connected_to_point(gpkg_paths, lon: float, lat: float, near_m=150, expand_within_m=5000):
    try:
        import geopandas as gpd, pandas as pd, fiona, os
    except Exception:
        return None
    from shapely.geometry import Point
    if isinstance(gpkg_paths, (str, Path)):
        gpkg_paths = [str(gpkg_paths)]
    pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(3857)
    buf_near = pt.buffer(near_m).to_crs(4326).iloc[0]
    buf_far = pt.buffer(expand_within_m).to_crs(4326).iloc[0]
    layers = []
    for path in gpkg_paths:
        if not os.path.exists(path):
            continue
        for lyr in fiona.listlayers(path):
            try:
                gdf = gpd.read_file(path, layer=lyr)
                if gdf.empty:
                    continue
                gtype = gdf.geometry.iloc[0].geom_type.lower()
                if "line" not in gtype:
                    continue
                gdf = gdf.set_crs("EPSG:4326", allow_override=True) if gdf.crs is None else gdf.to_crs("EPSG:4326")
                gdf = gdf[gdf.intersects(buf_far)]
                if not gdf.empty:
                    layers.append(gdf)
            except Exception:
                continue
    if not layers:
        return None
    import pandas as pd
    gdf = pd.concat(layers, ignore_index=True)
    seed = gdf[gdf.intersects(buf_near)]
    if seed.empty:
        try:
            d = gdf.geometry.distance(Point(lon, lat))
            seed = gdf.nsmallest(20, d)
        except Exception:
            seed = gdf.head(50)
    try:
        current = seed.copy()
        changed = True
        while changed:
            changed = False
            rest = gdf[~gdf.index.isin(current.index)]
            if rest.empty:
                break
            join = gpd.sjoin(rest, current[["geometry"]], how="inner", predicate="intersects")
            if not join.empty:
                add = rest.loc[sorted(set(join.index))]
                prev = len(current)
                current = pd.concat([current, add]).drop_duplicates()
                changed = len(current) > prev
        result = current
    except Exception:
        result = seed
    return result.to_json()


def _geojson_nearest_point_only(path: str, lon: float, lat: float):
    try:
        import geopandas as gpd
        from shapely.geometry import Point
        import os
    except Exception:
        return None
    if not path or not os.path.exists(path):
        return None
    try:
        gdf = gpd.read_file(path)
        if gdf.crs is None:
            gdf = gdf.set_crs("EPSG:4326", allow_override=True)
        else:
            gdf = gdf.to_crs("EPSG:4326")
        if gdf.empty:
            return None
        pt = Point(lon, lat)
        d = gdf.geometry.distance(pt)
        i = d.idxmin()
        return gdf.loc[[i]].to_json()
    except Exception:
        return None


class _CORSGeoJSONHandler(BaseHTTPRequestHandler):
    routes = {}

    def _write(self, code=200, ctype="application/geo+json; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

    def do_GET(self):
        # Serve libs with proper content-type
        if self.path.startswith('/lib/'):
            print('[HTTP] GET', self.path)
            data = self.routes.get(self.path)
            if data is not None:
                if self.path.endswith('.css'):
                    self._write(200, 'text/css; charset=utf-8')
                else:
                    self._write(200, 'application/javascript; charset=utf-8')
                self.wfile.write(data.encode('utf-8'))
                return

        data = self.routes.get(self.path)
        if data is None:
            self._write(404, "text/plain; charset=utf-8");
            self.wfile.write(b"Not found");
            return

        # Content-Type je nach Pfad wählen
        ctype = "application/geo+json; charset=utf-8"
        if self.path.endswith(".html"):
            ctype = "text/html; charset=utf-8"
        elif self.path.endswith(".png"):
            ctype = "image/png"
        elif self.path.endswith(".jpg") or self.path.endswith(".jpeg"):
            ctype = "image/jpeg"
        elif self.path.endswith(".svg"):
            ctype = "image/svg+xml"

        self._write(200, ctype)

        if isinstance(data, bytes):
            self.wfile.write(data)
        else:
            self.wfile.write(data.encode("utf-8"))

    def log_message(self, fmt, *args):
        return


def _fetch_maplibre_libs(payloads: dict):
    import urllib.request

    def _cache(route, url):
        try:
            with urllib.request.urlopen(url, timeout=20) as r:
                data = r.read()
            payloads[route] = data.decode("utf-8", errors="ignore")
            print("[LIB] cached", route, "from", url)
            return True
        except Exception as e:
            print("[LIB][ERR]", route, ":", e, "from", url)
            return False

    # Only MapLibre core (no Leaflet dependencies)
    _cache("/lib/maplibre-gl.js", "https://unpkg.com/maplibre-gl@3.6.1/dist/maplibre-gl.js")
    _cache("/lib/maplibre-gl.css", "https://unpkg.com/maplibre-gl@3.6.1/dist/maplibre-gl.css")


def _start_geojson_server(route_to_payload: dict):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    host, port = s.getsockname()
    s.close()

    class Handler(_CORSGeoJSONHandler): pass

    Handler.routes = route_to_payload
    srv = HTTPServer(("127.0.0.1", port), Handler)
    th = _threading_for_server.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    return f"http://127.0.0.1:{port}", srv, th


# ------------------------- Helper: clip & merge external layers -------------------------
def _merge_and_clip_pipelines(gpkg_list, lon, lat, radius_m):
    try:
        import geopandas as gpd, fiona, shapely
        from shapely.geometry import Point
    except Exception:
        return None
    try:
        buf = Point(lon, lat).buffer(radius_m / 111320.0)  # rough deg buffer
        gdfs = []
        for gpkg in (gpkg_list or []):
            if not gpkg:
                continue
            try:
                for lyr in fiona.listlayers(gpkg):
                    try:
                        gdf = gpd.read_file(gpkg, layer=lyr)
                        if gdf is None or gdf.empty:
                            continue
                        if gdf.crs is None:
                            gdf = gdf.set_crs("EPSG:4326", allow_override=True)
                        else:
                            gdf = gdf.to_crs("EPSG:4326")
                        gdf = gdf[~gdf.geometry.isna() & ~gdf.geometry.is_empty]
                        if gdf.empty:
                            continue
                        # keep points & lines; ignore polygons here
                        gdf = gdf[gdf.geometry.geom_type.isin(["Point", "MultiPoint", "LineString", "MultiLineString"])]
                        if gdf.empty:
                            continue
                        gdf = gdf[gdf.intersects(buf)]
                        if not gdf.empty:
                            gdfs.append(gdf)
                    except Exception:
                        continue
            except Exception:
                continue
        if not gdfs:
            return None
        import pandas as pd
        out = pd.concat(gdfs, ignore_index=True)
        return out.to_json()
    except Exception:
        return None


def _clip_lines_near_point(gpkg_path, lon, lat, radius_m):
    try:
        import geopandas as gpd, fiona
        from shapely.geometry import Point
    except Exception:
        return None
    try:
        if not gpkg_path:
            return None
        buf = Point(lon, lat).buffer(radius_m / 111320.0)
        gdfs = []
        for lyr in fiona.listlayers(gpkg_path):
            try:
                gdf = gpd.read_file(gpkg_path, layer=lyr)
                if gdf is None or gdf.empty:
                    continue
                gdf = gdf.set_crs("EPSG:4326", allow_override=True) if gdf.crs is None else gdf.to_crs("EPSG:4326")
                gdf = gdf[~gdf.geometry.isna() & ~gdf.geometry.is_empty]
                gdf = gdf[gdf.geometry.geom_type.isin(["Point", "MultiPoint", "LineString", "MultiLineString"])]
                if gdf.empty:
                    continue
                gdf = gdf[gdf.intersects(buf)]
                if not gdf.empty:
                    gdfs.append(gdf)
            except Exception:
                continue
        if not gdfs:
            return None
        import pandas as pd
        out = pd.concat(gdfs, ignore_index=True)
        return out.to_json()
    except Exception:
        return None


def _json_clip_features(json_path, lon, lat, radius_m):
    try:
        import json
        from shapely.geometry import shape, Point
    except Exception:
        return None
    try:
        p = Path(json_path)
        if not p.exists():
            return None
        data = json.loads(p.read_text(encoding="utf-8"))
        feats = data.get("features") or []
        buf = Point(lon, lat).buffer(radius_m / 111320.0)
        out_feats = []
        for f in feats:
            try:
                geom = f.get("geometry")
                if not geom:
                    continue
                g = shape(geom)
                if g.is_empty:
                    continue
                if g.geom_type not in ["Point", "MultiPoint", "LineString", "MultiLineString", "Polygon",
                                       "MultiPolygon"]:
                    continue
                if g.intersects(buf):
                    out_feats.append(f)
            except Exception:
                continue
        if not out_feats:
            return None
        return json.dumps({"type": "FeatureCollection", "features": out_feats})
    except Exception:
        return None


def _geojson_clip_radius(geojson_path, lon, lat, radius_m):
    try:
        import json
        import geopandas as gpd
        from shapely.geometry import shape, Point, mapping
        from pyproj import Transformer
    except Exception as e:
        print(f"[_geojson_clip_radius] Import error: {e}")
        return None
    try:
        if not Path(geojson_path).exists():
            print(f"[_geojson_clip_radius] File not found: {geojson_path}")
            return None
        print(f"[_geojson_clip_radius] Reading file...")

        with open(geojson_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        features = data.get('features', [])
        print(f"[_geojson_clip_radius] Loaded {len(features)} features")

        # Assume Lambert 93 (EPSG:2154) for French data
        transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)

        # Create buffer in EPSG:4326
        pt = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326")
        pt_3857 = pt.to_crs("EPSG:3857")
        buf_3857 = pt_3857.buffer(radius_m)
        buf_4326 = buf_3857.to_crs("EPSG:4326").iloc[0]

        valid_features = []
        for feat in features:
            try:
                geom = feat.get('geometry')
                if not geom:
                    continue

                # Transform coordinates from Lambert 93 to WGS84
                geom_type = geom['type']
                coords = geom['coordinates']

                def transform_coords(coords_in, depth=0):
                    if depth == 0 and geom_type == 'Point':
                        return list(transformer.transform(coords_in[0], coords_in[1]))
                    elif depth == 0 and geom_type in ['LineString', 'MultiPoint']:
                        return [list(transformer.transform(x, y)) for x, y in coords_in]
                    elif depth == 0 and geom_type == 'Polygon':
                        return [[list(transformer.transform(x, y)) for x, y in ring] for ring in coords_in]
                    elif depth == 0 and geom_type in ['MultiLineString', 'MultiPolygon']:
                        return [transform_coords(part, depth=1) for part in coords_in]
                    elif depth == 1 and geom_type == 'MultiLineString':
                        return [list(transformer.transform(x, y)) for x, y in coords_in]
                    elif depth == 1 and geom_type == 'MultiPolygon':
                        return [[list(transformer.transform(x, y)) for x, y in ring] for ring in coords_in]
                    return coords_in

                new_coords = transform_coords(coords)
                new_geom = {'type': geom_type, 'coordinates': new_coords}

                g = shape(new_geom)
                if g.is_empty or not g.is_valid:
                    continue

                if g.intersects(buf_4326):
                    feat['geometry'] = new_geom
                    valid_features.append(feat)
            except Exception:
                continue

        print(f"[_geojson_clip_radius] After filtering: {len(valid_features)} features")
        if not valid_features:
            return None

        result = json.dumps({"type": "FeatureCollection", "features": valid_features})
        print(f"[_geojson_clip_radius] ✓ Returning {len(result)} bytes")
        return result
    except Exception as e:
        print(f"[_geojson_clip_radius] Error: {e}")
        import traceback
        traceback.print_exc()
        return None


# ------------------------- Shapes laden & finden -------------------------
SHAPES_GDF: Optional[gpd.GeoDataFrame] = None


def load_shapes():
    """Lädt alle Polygon-Layer aus dem GPKG (falls mehrere) zusammen."""
    global SHAPES_GDF
    if not os.path.exists(GPKG_PATH):
        SHAPES_GDF = None
        return
    layers = []
    try:
        layers = fiona.listlayers(GPKG_PATH)
    except Exception:
        layers = []
    gdfs = []
    if layers:
        for lyr in layers:
            try:
                g = gpd.read_file(GPKG_PATH, layer=lyr)
                if g.empty or "geometry" not in g: continue
                if g.crs is None:
                    g = g.set_crs("EPSG:4326", allow_override=True)
                else:
                    g = g.to_crs("EPSG:4326")
                g = g[g.geometry.notna()]
                g = g[g.geometry.type.isin(["Polygon", "MultiPolygon"])]
                if not g.empty:
                    gdfs.append(g)
            except Exception:
                continue
    if not gdfs:
        # Fallback: ohne Layerangabe
        g = gpd.read_file(GPKG_PATH)
        if g.crs is None:
            g = g.set_crs("EPSG:4326", allow_override=True)
        else:
            g = g.to_crs("EPSG:4326")
        g = g[g.geometry.notna()]
        g = g[g.geometry.type.isin(["Polygon", "MultiPolygon"])]
        gdfs = [g] if not g.empty else []

    if gdfs:
        SHAPES_GDF = pd.concat(gdfs, ignore_index=True)
    else:
        SHAPES_GDF = None


def find_nearest_shape(lon: float, lat: float) -> Optional[Polygon]:
    """Nächstes WWTP-Polygon zum Punkt (lon, lat) .
    - Reprojiziert nach EPSG:3857 und wählt die minimale Distanz.
    - Repariert ungültige Geometrien (buffer(0)).
    """
    if SHAPES_GDF is None or SHAPES_GDF.empty:
        log("[WWTPS] Keine Shapes geladen.")
        return None
    try:
        shp = SHAPES_GDF.copy()
        if shp.crs is None:
            shp = shp.set_crs("EPSG:4326", allow_override=True)
        shp_3857 = shp.to_crs("EPSG:3857")
        pt_3857 = gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs("EPSG:3857").iloc[0]
        # Distanz-basiert (O(n) – bei ~300 Features absolut ok)
        dists = shp_3857.distance(pt_3857)
        idx = int(dists.idxmin())
        geom = SHAPES_GDF.geometry.iloc[idx]
        if geom is None or geom.is_empty:
            return None
        # Ungültige Geometrie reparieren
        try:
            if not geom.is_valid:
                geom = geom.buffer(0)
        except Exception:
            pass
        # größtes Teilpolygon, falls MultiPolygon
        if isinstance(geom, MultiPolygon):
            try:
                geom = max(list(geom.geoms), key=lambda g: g.area)
            except Exception:
                geom = list(geom.geoms)[0]
        # Debug-Ausgabe
        try:
            c = geom.centroid
            print(f"[WWTPS] Nearest polygon gefunden. Centroid: ({c.y:.5f}, {c.x:.5f})")
        except Exception:
            log("[WWTPS] Nearest polygon gefunden.")
        return geom
    except Exception as e:
        log("[WWTPS] find_nearest_shape() fehlgeschlagen:", e)
        return None


# ------------------------- Excel → Tabelle -------------------------
CAND_ID = ["id"]
CAND_CODE = ["uwwtd_code", "uwwtd code", "code"]
CAND_NAME = ["plant name", "name", "anlagenname"]
CAND_LAT = ["lat", "latitude"]
CAND_LON = ["lon", "longitude"]


def _norm_cols(df: pd.DataFrame) -> Dict[str, str]:
    return {str(c).strip().lower(): c for c in df.columns}


def _pick(df: pd.DataFrame, cands: List[str]) -> Optional[str]:
    m = _norm_cols(df)
    for k in cands:
        if k in m: return m[k]
    for k, c in m.items():
        if any(x in k for x in cands): return c
    return None


def load_facilities(path: str) -> gpd.GeoDataFrame:
    xls = pd.ExcelFile(path)
    chosen = None
    for s in xls.sheet_names:
        df = xls.parse(s)
        if df.empty: continue
        if _pick(df, CAND_NAME) and (_pick(df, CAND_LAT) and _pick(df, CAND_LON)):
            chosen = df;
            break
    if chosen is None:
        chosen = xls.parse(xls.sheet_names[0])

    id_col = _pick(chosen, CAND_ID)
    code_col = _pick(chosen, CAND_CODE)
    name_col = _pick(chosen, CAND_NAME)
    lat_col = _pick(chosen, CAND_LAT)
    lon_col = _pick(chosen, CAND_LON)

    df = chosen.copy()
    if id_col is None:
        df["_id"] = range(1, len(df) + 1);
        id_col = "_id"

    df["_lat"] = pd.to_numeric(df[lat_col], errors="coerce")
    df["_lon"] = pd.to_numeric(df[lon_col], errors="coerce")

    gdf = gpd.GeoDataFrame({
        "id": df[id_col],
        "uwwtd_code": df[code_col] if code_col else None,
        "name": df[name_col] if name_col else None,
    }, geometry=[Point(xy) if pd.notna(xy[0]) and pd.notna(xy[1]) else None
                 for xy in zip(df["_lon"], df["_lat"])], crs="EPSG:4326")
    return gdf.dropna(subset=["geometry"]).reset_index(drop=True)


# ------------------------- Regionserkennung -------------------------
class BBox:
    def __init__(self, min_lat, min_lon, max_lat, max_lon):
        self.min_lat = min_lat;
        self.min_lon = min_lon;
        self.max_lat = max_lat;
        self.max_lon = max_lon

    def contains(self, lat, lon): return self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon


COUNTRIES = {
    "DE": BBox(47.27, 5.86, 55.09, 15.04),
    "FR": BBox(41.33, -5.14, 51.09, 9.56),
    "HR": BBox(42.18, 13.49, 46.86, 19.45),
    "PL": BBox(49.0, 14.1, 55.1, 24.2),
    "ES": BBox(27.6, -18.2, 43.8, 4.4),
    "CH": BBox(45.8, 5.9, 47.9, 10.5),
    "HU": BBox(45.7, 16.1, 48.6, 22.9),
    "NL": BBox(50.7, 3.2, 53.7, 7.3),
}
DE_STATES = {
    "Berlin": BBox(52.33, 13.08, 52.69, 13.77),
    "Bayern": BBox(47.27, 8.98, 50.56, 13.84),
    "Bremen": BBox(53.0, 8.1, 53.9, 9.1),
    "Niedersachsen": BBox(51.2, 6.5, 53.9, 11.7),
}


def detect_region(lat: float, lon: float) -> Tuple[Optional[str], Optional[str]]:
    for st, bb in DE_STATES.items():
        if bb.contains(lat, lon): return "DE", st
    for cc, bb in COUNTRIES.items():
        if bb.contains(lat, lon): return cc, None
    return None, None


# ------------------------- Capabilities / CRS -------------------------
def _base_wms(url: str) -> str:
    return url.split("?")[0] + "?"


def list_layers_with_crs(base_url: str, version="1.3.0") -> List[Dict[str, Any]]:
    cap = f"{_base_wms(base_url)}REQUEST=GetCapabilities&SERVICE=WMS&VERSION={version}"
    try:
        r = requests.get(cap, timeout=20);
        r.raise_for_status()
        root = ET.fromstring(r.text)
    except Exception:
        return []
    layers = []
    for node in root.iter():
        if not node.tag.lower().endswith("layer"): continue
        nm = None;
        title = None;
        crs_list = []
        for ch in node:
            tag = ch.tag.lower()
            if tag.endswith("name") and ch.text:
                nm = ch.text.strip()
            elif tag.endswith("title") and ch.text:
                title = ch.text.strip()
            elif tag.endswith("crs") or tag.endswith("srs"):
                if ch.text: crs_list += [c.strip().upper() for c in ch.text.split()]
        if nm:
            layers.append({"name": nm, "title": title or nm, "crs": list(set(crs_list))})
    return layers


def choose_layer_and_crs(base_url: str, fixed_layer: Optional[str], version="1.3.0",
                         prefer_crs: Optional[List[str]] = None,
                         exclude_name_contains: Optional[List[str]] = None) -> Tuple[Optional[str], Optional[str]]:
    layers = list_layers_with_crs(base_url, version=version)
    if not layers: return None, None

    # exact-match für festen Layer (case-insensitive)
    if fixed_layer:
        for L in layers:
            if L["name"].strip().upper() == fixed_layer.strip().upper():
                for crs in (prefer_crs or CRS_PREF):
                    if crs in L["crs"]: return L["name"], crs
                return L["name"], (L["crs"][0] if L["crs"] else None)

    excl = [s.upper() for s in (exclude_name_contains or [])]

    def score(L):
        txt = (L["name"] + " " + L["title"]).upper()
        if any(bad in txt for bad in excl):  # z. B. INFO
            return -99
        pos = any(k in txt for k in POS_KEYS)
        neg = any(k in txt for k in NEG_KEYS)
        return (1 if pos else 0) - (1 if neg else 0)

    cand_layers = sorted(layers, key=score, reverse=True)
    crs_pref = prefer_crs or CRS_PREF
    for L in cand_layers:
        if any(bad in (L["name"] + " " + L["title"]).upper() for bad in excl):
            continue
        for crs in crs_pref:
            if crs in L["crs"]:
                return L["name"], crs
    L0 = cand_layers[0]
    return L0["name"], (L0["crs"][0] if L0["crs"] else None)


def meters_to_deg(lat_deg: float, dx: float, dy: float) -> Tuple[float, float]:
    dlat = dy / 110574.0
    dlon = dx / (111320.0 * max(0.0001, math.cos(math.radians(lat_deg))))
    return dlon, dlat


def build_bbox(lon: float, lat: float, crs_code: str, half_m: float) -> Tuple[float, float, float, float]:
    crs_up = crs_code.upper()
    if crs_up in ("EPSG:4326", "CRS:84"):
        dlon, dlat = meters_to_deg(lat, half_m, half_m)
        return (lon - dlon, lat - dlat, lon + dlon, lat + dlat)
    tr = Transformer.from_crs("EPSG:4326", crs_up, always_xy=True)
    cx, cy = tr.transform(lon, lat)
    return (cx - half_m, cy - half_m, cx + half_m, cy + half_m)


def axis_order_is_latlon(version: str, crs_code: str) -> bool:
    """WMS 1.3.0 + EPSG:4326 ⇒ BBOX lat,lon. 1.1.1 ⇒ immer lon,lat."""
    v = (version or "1.3.0").strip()
    if v.startswith("1.1"): return False
    return crs_code.upper() == "EPSG:4326"


def build_getmap_url(url: str, version: str, layer: str, crs_code: str,
                     width: int, height: int, lon: float, lat: float,
                     half_m: float, fmt="image/jpeg", vendor: Optional[Dict[str, str]] = None,
                     provider: str = "") -> Tuple[str, Tuple[float, float, float, float], str]:
    """
    Gibt (url, bbox_xy, used_version) zurück.
    bbox_xy ist IMMER (minx, miny, maxx, maxy) im numerischen Koordinatensystem des Request-CRS.
    Die String-Reihenfolge (lat/lon) für 1.3.0+4326 wird nur für den BBOX-Parameter angewandt.
    """
    # Spezial: Wenn 4326 gewählt, erzwinge 1.1.1 (lon,lat) – universell, nicht nur PL.
    v = version
    if crs_code.upper() == "EPSG:4326":
        v = "1.1.1"

    minx, miny, maxx, maxy = build_bbox(lon, lat, crs_code, half_m)
    if axis_order_is_latlon(v, crs_code):
        bbox_param = f"{miny},{minx},{maxy},{maxx}";
        crs_param = "crs"
    else:
        bbox_param = f"{minx},{miny},{maxx},{maxy}";
        crs_param = ("srs" if v.startswith("1.1") else "crs")
    params = {
        "service": "WMS", "request": "GetMap", "version": v,
        "layers": layer, "styles": "", "width": str(width), "height": str(height),
        "format": fmt, "transparent": "FALSE", crs_param: crs_code, "bbox": bbox_param
    }
    if vendor:
        params.update(vendor)
    qs = "&".join(f"{k}={requests.utils.quote(str(v))}" for k, v in params.items())
    return (url if url.endswith("?") else url + "?") + qs, (minx, miny, maxx, maxy), v


def eea_service() -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(EEA_CAPS, timeout=20);
        r.raise_for_status()
        root = ET.fromstring(r.text)
        version = root.attrib.get("version", "1.3.0")
        layer = None
        for node in root.iter():
            if node.tag.lower().endswith("layer"):
                nm = node.find(".//{*}Name")
                tt = node.find(".//{*}Title")
                name = nm.text.strip() if nm is not None and nm.text else None
                text = (name or "") + " " + (tt.text if tt is not None and tt.text else "")
                if name and any(k in text.upper() for k in ["ORTHO", "ORTHOIMAGERY", "ORTHOPHOTO", "RGB"]):
                    layer = name;
                    break
        return {"url": _base_wms(EEA_CAPS), "version": version, "layer": layer or "",
                "crs": "EPSG:3857", "format": "image/jpeg", "provider": "EEA"}
    except Exception:
        return None


def pick_services(lat: float, lon: float) -> List[Dict[str, Any]]:
    cc, state = detect_region(lat, lon)
    out: List[Dict[str, Any]] = []

    def add_entry(key: str):
        base = RAW_ENDPOINTS[key]
        fixed_layer, fixed_ver = FIXED_LAYERS.get(key, (None, "1.3.0"))
        tuning = TUNING.get(key, {})
        prefer = tuning.get("prefer_crs")
        exclude = ["INFO"] if key == "Niedersachsen" else None
        layer, crs = choose_layer_and_crs(base, fixed_layer, version=fixed_ver,
                                          prefer_crs=prefer, exclude_name_contains=exclude)

        if key == "PL" and crs == "EPSG:4326":
            # Versuche 3857 oder 2180
            for alt in ["EPSG:3857", "EPSG:2180"]:
                if alt in (prefer or []) or True:
                    crs = alt;
                    break
        out.append({
            "provider": key,
            "url": _base_wms(base),
            "version": fixed_ver,
            "layer": layer or "",
            "crs": crs or "EPSG:3857",
            "format": "image/jpeg",
            "half_size_m": tuning.get("half_size_m", DEFAULT_HALF_SIZE_M),
            "req_w": tuning.get("req_w", DISPLAY_W * 2),
            "req_h": tuning.get("req_h", DISPLAY_H * 2),
            "vendor": tuning.get("vendor", {}),
        })

    if cc == "DE" and state and state in RAW_ENDPOINTS:
        add_entry(state)
    if cc and cc in RAW_ENDPOINTS:
        add_entry(cc)
    eea = eea_service()
    if eea:
        eea.update({"half_size_m": DEFAULT_HALF_SIZE_M, "req_w": DISPLAY_W * 2, "req_h": DISPLAY_H * 2, "vendor": {}})
        out.append(eea)
    eox = EOX_WMS.copy()
    eox.update({"half_size_m": DEFAULT_HALF_SIZE_M, "req_w": DISPLAY_W * 2, "req_h": DISPLAY_H * 2, "vendor": {}})
    out.append(eox)

    # Doppelte filtern
    seen = set();
    res = []
    for s in out:
        key = (s["url"], s.get("layer", ""), s["version"], s.get("crs", ""))
        if key in seen: continue
        seen.add(key);
        res.append(s)
    return res


def image_looks_blank(img: Image.Image, var_thresh: float = 20.0) -> bool:
    try:
        stat = ImageStat.Stat(img)
        avg_var = sum(stat.var) / len(stat.var)
        return avg_var < var_thresh
    except Exception:
        return False


# Polygon-Overlay ins Bild zeichnen (immer im numerischen XY-System des Request-CRS)
def draw_polygon_overlay(img: Image.Image, bbox_xy: Tuple[float, float, float, float], crs_code: str,
                         poly: Polygon) -> Image.Image:
    if poly is None:
        return img
    # Projektions-Transformer: 4326 -> Ziel-CRS
    tr = Transformer.from_crs("EPSG:4326", crs_code, always_xy=True)

    def project_ring(ring):
        return [tr.transform(x, y) for x, y in ring.coords]

    minx, miny, maxx, maxy = bbox_xy
    W, H = img.size
    sx = W / (maxx - minx)
    sy = H / (maxy - miny)

    def to_px(coords_xy):
        return [(int((X - minx) * sx), int((maxy - Y) * sy)) for (X, Y) in coords_xy]

    overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(overlay, "RGBA")
    fill = (65, 105, 225, int(255 * 0.15))  # royal blue 15%
    outline = (65, 105, 225, int(255 * 0.90))

    # Exterior
    if poly.exterior:
        ext_xy = project_ring(poly.exterior)
        ext_px = to_px(ext_xy)
        if len(ext_px) >= 3:
            d.polygon(ext_px, fill=fill, outline=outline, width=3)
    # Holes
    for inner in poly.interiors:
        in_xy = project_ring(inner)
        in_px = to_px(in_xy)
        if len(in_px) >= 3:
            d.polygon(in_px, fill=(0, 0, 0, 0), outline=outline, width=1)

    return Image.alpha_composite(img.convert("RGBA"), overlay).convert("RGB")


# ------------------------- ESRI World_Imagery (Tiles) -------------------------

ESRI_TILE_URL = "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
TILE_SIZE = 256
WORLD_M = 40075016.68557849


def _lonlat_to_mercator(lon: float, lat: float):
    # clamp latitude to mercator bounds
    lat = max(min(lat, 85.05112878), -85.05112878)
    x = lon * WORLD_M / 360.0
    y = math.log(math.tan((90 + lat) * math.pi / 360.0)) / (math.pi / 180.0)
    y = y * WORLD_M / 360.0
    return x, y


def _mercator_to_pixel(x: float, y: float, z: int):
    res = WORLD_M / (TILE_SIZE * (2 ** z))
    px = (x + WORLD_M / 2.0) / res
    py = (WORLD_M / 2.0 - y) / res  # y downwards
    return px, py


def _choose_zoom_for_span(center_lat: float, span_m: float, target_px: int) -> int:
    # meters per pixel ≈ 156543.0339 * cos(lat) / 2^z
    for z in range(22, 0, -1):
        mpp = 156543.03392804097 * math.cos(math.radians(center_lat)) / (2 ** z)
        if span_m / mpp <= target_px:
            return z
    return 1


def _fetch_esri_tile(z: int, x: int, y: int):
    url = ESRI_TILE_URL.format(z=z, x=x, y=y)
    try:
        r = requests.get(url, timeout=20);
        r.raise_for_status()
        return Image.open(io.BytesIO(r.content)).convert("RGB")
    except Exception:
        return None


def fetch_esri_image(center_lat: float, center_lon: float, overlay_poly: Optional[Polygon]):
    """
    Holt ein Luftbild von ESRI World_Imagery um (center_lon, center_lat),
    nutzt DEFAULT_HALF_SIZE_M als halbe Kantenlänge des Zielausschnitts,
    zeichnet optional das Polygon-Overlay, und skaliert auf DISPLAY_W x DISPLAY_H.
    """
    half_m = DEFAULT_HALF_SIZE_M * 0.9  # analog zum bisherigen Standard
    span_m_w = half_m * 2
    span_m_h = half_m * 2

    # Wir holen etwas größer (2x), danach resize wie bisher
    OUT_W = DISPLAY_W * 2
    OUT_H = DISPLAY_H * 2

    # Bestimme Zoom passend zur Breite
    z = _choose_zoom_for_span(center_lat, span_m_w, OUT_W)

    # BBox im 3857 um den Mittelpunkt
    cxm, cym = _lonlat_to_mercator(center_lon, center_lat)
    minx = cxm - half_m
    maxx = cxm + half_m
    miny = cym - half_m * (OUT_H / OUT_W)
    maxy = cym + half_m * (OUT_H / OUT_W)
    bbox_xy = (minx, miny, maxx, maxy)  # 3857

    # Erforderliche Tiles bestimmen
    tl_px, tl_py = _mercator_to_pixel(minx, maxy, z)  # top-left
    br_px, br_py = _mercator_to_pixel(maxx, miny, z)  # bottom-right

    tx0, ty0 = int(tl_px // TILE_SIZE), int(tl_py // TILE_SIZE)
    tx1, ty1 = int(br_px // TILE_SIZE), int(br_py // TILE_SIZE)

    mosaic_w = (tx1 - tx0 + 1) * TILE_SIZE
    mosaic_h = (ty1 - ty0 + 1) * TILE_SIZE
    mosaic = Image.new("RGB", (mosaic_w, mosaic_h), (245, 245, 245))

    for ty in range(ty0, ty1 + 1):
        for tx in range(tx0, tx1 + 1):
            tile = _fetch_esri_tile(z, tx, ty)
            if tile is None:
                tile = Image.new("RGB", (TILE_SIZE, TILE_SIZE), (245, 245, 245))
            ox = (tx - tx0) * TILE_SIZE
            oy = (ty - ty0) * TILE_SIZE
            mosaic.paste(tile, (ox, oy))

    # Crop auf gewünschte BBox
    mosaic_abs_px = tx0 * TILE_SIZE
    mosaic_abs_py = ty0 * TILE_SIZE
    crop_left = int(round(tl_px - mosaic_abs_px))
    crop_top = int(round(tl_py - mosaic_abs_py))
    crop_right = int(round(br_px - mosaic_abs_px))
    crop_bottom = int(round(br_py - mosaic_abs_py))
    crop_left = max(0, min(crop_left, mosaic_w))
    crop_top = max(0, min(crop_top, mosaic_h))
    crop_right = max(0, min(crop_right, mosaic_w))
    crop_bottom = max(0, min(crop_bottom, mosaic_h))

    view = mosaic.crop((crop_left, crop_top, crop_right, crop_bottom)).resize((OUT_W, OUT_H), Image.BILINEAR)

    # Overlay: benutze vorhandene Routine mit 3857-BBox
    if overlay_poly is not None:
        view = draw_polygon_overlay(view, bbox_xy, "EPSG:3857", overlay_poly)

    # Finale Größe wie bisher
    view = view.resize((DISPLAY_W, DISPLAY_H), Image.LANCZOS)
    return view, "ESRI World_Imagery"


def fetch_wms_image(center_lat: float, center_lon: float, overlay_poly: Optional[Polygon]) -> Tuple[
    Optional[Image.Image], Optional[str]]:
    for svc in pick_services(center_lat, center_lon):
        try:
            if not svc.get("layer"): continue
            url, bbox_xy, used_version = build_getmap_url(
                svc["url"], svc["version"], svc["layer"], svc["crs"],
                svc["req_w"], svc["req_h"], center_lon, center_lat, svc["half_size_m"],
                fmt=svc["format"], vendor=svc.get("vendor"), provider=svc["provider"]
            )
            r = requests.get(url, timeout=30);
            r.raise_for_status()
            img = Image.open(io.BytesIO(r.content)).convert("RGB")
            if image_looks_blank(img): continue
            if overlay_poly is not None:
                log('[WWTPS] Overlay wird gezeichnet...')
                img = draw_polygon_overlay(img, bbox_xy, svc["crs"], overlay_poly)
            img = img.resize((DISPLAY_W, DISPLAY_H), Image.LANCZOS)
            return img, svc.get("provider", "WMS")
        except Exception:
            continue
    return None, None


# ------------------------- GUI -------------------------
class App(tk.Tk):
    SCORE_COLORS = {
        # H2 Logistics
        "logistic_score": "#b8e6b8",
        "h2_logistics_score": "#b8e6b8",
        "logistics_score": "#b8e6b8",
        # Stromnetzanbindung - Yellow
        "grid_electricity_score": "#fff3a0",
        # H2 Renewables Potential -
        "renewables_potential_score": "#b8e6b8",
        "h2_renewables_potential_score": "#b8e6b8",
        "h2_renewables_score": "#b8e6b8",
        "renewables_score": "#b8e6b8",
        # Fernwärme - Orange
        "district_heating_score": "#ffb380",
        # Oxygen - Light Blue
        "oxygen_score": "#a8d8f0",
        # Risks
        "risk_score": "#f5a8a8",
        # Final Score - Light Purple/Violet
        "final_score": "#d4c5f9",
        # Mean Rank, etc. - White
        "mean_rank": "#ffffff",
        "rank_std_dev": "#ffffff",
        "top_20_frequency": "#ffffff",
    }

    # --- Lade overlay helpers ---
    def _show_loading(self, message: str = "Loading data..."):
        if getattr(self, "_loading_overlay", None):
            return
        self._loading_overlay = tk.Frame(self, bg="#ffffff")
        self._loading_overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
        self._loading_label = ttk.Label(self._loading_overlay, text=message)
        self._loading_label.place(relx=0.5, rely=0.5, anchor="s")
        self._loading_bar = ttk.Progressbar(self._loading_overlay, mode="indeterminate", length=180)
        self._loading_bar.place(relx=0.5, rely=0.5, anchor="n", y=6)
        try:
            self._loading_bar.start(10)
        except Exception:
            pass

    def _hide_loading(self):
        if getattr(self, "_loading_overlay", None):
            try:
                if getattr(self, "_loading_bar", None):
                    self._loading_bar.stop()
            except Exception:
                pass
            self._loading_overlay.destroy()
            self._loading_overlay = None
            self._loading_bar = None
            self._loading_label = None

    def _read_excel_to_df(self, path: Path) -> pd.DataFrame:
        # Base facilities with geometry (lat/lon)
        gdf = load_facilities(str(path))
        df = pd.DataFrame(gdf.drop(columns=gdf.geometry.name))
        df["lon"] = gdf.geometry.x
        df["lat"] = gdf.geometry.y
        # Core columns
        cols = [c for c in ["id", "uwwtd_code", "name", "lat", "lon"] if c in df.columns]
        df = df[cols].copy()

        try:

            xls = pd.ExcelFile(str(path))
            print(f"[DEBUG] Available sheets: {xls.sheet_names}")

            calc_sheet_name = None
            for sheet in xls.sheet_names:
                if "calculated" in sheet.lower() and "value" in sheet.lower():
                    calc_sheet_name = sheet
                    break

            if not calc_sheet_name:
                log("[INFO] No 'Calculated Values' sheet found")
            else:
                print(f"[DEBUG] Loading sheet: {calc_sheet_name}")
                calc_vals = pd.read_excel(str(path), sheet_name=calc_sheet_name)

                print(f"[DEBUG] Original columns in {calc_sheet_name}: {list(calc_vals.columns)}")
                print(f"[DEBUG] Number of rows: {len(calc_vals)}")

                # Normalize column names
                def norm(s):
                    import re as _re
                    return _re.sub(r"[^0-9a-zA-Z]+", "_", str(s).strip().lower()).strip("_")

                calc_vals.columns = [norm(c) for c in calc_vals.columns]
                print(f"[DEBUG] Normalized columns: {list(calc_vals.columns)}")

                key = None
                if "uwwtd_code" in calc_vals.columns and "uwwtd_code" in df.columns:
                    key = "uwwtd_code"
                elif "id" in calc_vals.columns and "id" in df.columns:
                    key = "id"
                elif "name" in calc_vals.columns and "name" in df.columns:
                    key = "name"

                print(f"[DEBUG] Merge key: {key}")
                print(f"[DEBUG] df columns: {list(df.columns)}")
                print(f"[DEBUG] calc_vals columns: {list(calc_vals.columns)}")

                if key:

                    exclude_cols = [key]
                    score_cols = [c for c in calc_vals.columns if c not in exclude_cols]
                    merge_cols = [key] + score_cols
                    calc_subset = calc_vals[merge_cols]

                    print(f"[DEBUG] Merging {len(merge_cols)} columns: {merge_cols}")

                    df = df.merge(calc_subset, on=key, how="left")
                    print(f"[DEBUG] Final df columns after merge: {list(df.columns)}")
                else:
                    log("[INFO] No matching key column found for merge")
        except Exception as e:
            print(f"[ERROR] Could not load Calculated Values sheet: {e}")
            import traceback
            traceback.print_exc()

        self._tech_by_key = {}
        try:
            tech = pd.read_excel(str(path), sheet_name="Technical Data - Plant Metrics")

            def norm(s):
                import re as _re
                return _re.sub(r"[^0-9a-zA-Z]+", "_", str(s).strip().lower()).strip("_")

            tech.columns = [norm(c) for c in tech.columns]

            key = None
            if "uwwtd_code" in tech.columns and "uwwtd_code" in df.columns:
                key = "uwwtd_code"
            elif "id" in tech.columns and "id" in df.columns:
                key = "id"

            cap_col = "capacity_pe" if "capacity_pe" in tech.columns else None
            if not cap_col:

                for alt in ["capacity", "design_capacity_pe", "cap_pe"]:
                    if alt in tech.columns:
                        cap_col = alt
                        break
            oz_col = None
            for cand in ["ozonation", "ozone", "ozonation_present", "ozone_present"]:
                if cand in tech.columns:
                    oz_col = cand
                    break

            if key:
                for _, r in tech.iterrows():
                    k = r.get(key)
                    if k is None:
                        continue
                    entry = {}
                    if cap_col is not None and cap_col in r:
                        entry["capacity_pe"] = r[cap_col]
                    if oz_col is not None and oz_col in r:
                        entry["ozonation"] = r[oz_col]
                    if entry:
                        self._tech_by_key[str(k)] = entry
        except Exception:

            self._tech_by_key = {}

        return df

    def _setup_table_from_df(self, df: pd.DataFrame):
        self.df = df
        #
        if "uwwtd_code" in df.columns:
            df["country"] = df["uwwtd_code"].astype(str).str[:2]
        else:
            df["country"] = ""

        #
        for col in df.columns:
            if "score" in col.lower():
                df[col] = pd.to_numeric(df[col], errors='coerce').round(5)
            elif "rank" in col.lower():
                df[col] = pd.to_numeric(df[col], errors='coerce').round(0).astype('Int64')

        # D
        base_cols = ["id", "country", "name"]
        exclude_cols = ["uwwtd_code", "lat", "lon", "geometry", "rank", "score_ci_lower", "score_ci_upper",
                        "rank_std_dev", "rank_ci_width", "top_20_frequency"]  # exclude CI and std dev columns

        #
        score_order = [
            "final_score",
            "h2_renewables_potential_score", "renewables_potential_score", "h2_renewables_score", "renewables_score",
            # H2
            "h2_logistics_score", "logistic_score", "logistics_score",  # H2
            "grid_electricity_score",
            "district_heating_score",
            #
            "oxygen_score",
        ]

        #
        all_score_cols = [c for c in df.columns if
                          "score" in c.lower() and c not in base_cols and c not in exclude_cols]

        #
        score_cols = [c for c in score_order if c in all_score_cols]
        score_cols += [c for c in all_score_cols if c not in score_cols]

        ranking_cols = [c for c in ["mean_rank"] if c in df.columns]

        #
        other_cols = [c for c in df.columns if
                      c not in base_cols and c not in exclude_cols and c not in score_cols and c not in ranking_cols]

        #
        display_cols = base_cols + score_cols + other_cols + ranking_cols

        self._setup_tree(display_cols)
        for i in self.tree.get_children():
            self.tree.delete(i)
        for _, r in df.iterrows():
            self.tree.insert("", tk.END, values=[r[c] if c in r else "" for c in display_cols])

    def _start_initial_load(self):
        # s
        self._show_loading("Lade Daten...")

        def worker():
            err = None
            df = None
            try:
                #

                load_shapes()
                df = self._read_excel_to_df(Path(XLSX_PATH))
            except Exception as e:
                err = e

            def on_done():
                if err is None and df is not None:
                    self._setup_table_from_df(df)
                else:
                    #
                    try:
                        for w in self._loading_overlay.winfo_children():
                            w.destroy()
                        msg = ttk.Label(self._loading_overlay, text=f"Error while loading: {err}", foreground="red")
                        msg.place(relx=0.5, rely=0.5, anchor="center")
                    except Exception:
                        pass
                self._hide_loading()

            self.after(0, on_done)

        threading.Thread(target=worker, daemon=True).start()

    def __init__(self) -> None:
        super().__init__()
        self.title("Plant Browser (Excel)")
        self.withdraw()
        self.update_idletasks()
        sw = self.winfo_screenwidth();
        sh = self.winfo_screenheight()
        #
        win_w = min(2200, sw - 100)  #
        win_h = 780
        x = max((sw - win_w) // 2, 0);
        y = max((sh - win_h) // 2, 0)
        self.geometry(f"{win_w}x{win_h}+{x}+{y}")
        self.resizable(True, True)
        self.deiconify()
        style = ttk.Style(self)
        if "vista" in style.theme_names():
            style.theme_use("vista")
        elif "clam" in style.theme_names():
            style.theme_use("clam")
        self._build_header()
        self._build_table()
        self._start_initial_load()

    def _build_header(self):
        frm = ttk.Frame(self, padding=(10, 8));
        frm.pack(side=tk.TOP, fill=tk.X)

        #
        ttk.Label(frm, text="Ranked Dataset:", width=15).pack(side=tk.LEFT, padx=(0, 4))

        #
        self.xlsx_options = get_available_ranked_files()
        default_key = list(self.xlsx_options.keys())[0] if self.xlsx_options else "EHB"

        self.dataset_var = tk.StringVar(value=default_key)
        dataset_combo = ttk.Combobox(frm, textvariable=self.dataset_var,
                                     values=list(self.xlsx_options.keys()),
                                     state="readonly", width=25)  #
        dataset_combo.pack(side=tk.LEFT, padx=(0, 10))
        dataset_combo.bind("<<ComboboxSelected>>", self._on_dataset_change)

        #
        ttk.Button(frm, text="🔄", command=self._refresh_datasets, width=3).pack(side=tk.LEFT, padx=(0, 10))

        ttk.Label(frm, text="Excel file:", width=10).pack(side=tk.LEFT)
        default_path = self.xlsx_options.get(default_key, XLSX_PATH)
        self.path_var = tk.StringVar(value=default_path)
        ttk.Entry(frm, textvariable=self.path_var).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 6))
        ttk.Button(frm, text="Browse...", command=self._browse).pack(side=tk.LEFT)

    def _refresh_datasets(self):
        """Refresh the list of available datasets"""
        self.xlsx_options = get_available_ranked_files()

        #
        for widget in self.winfo_children():
            if isinstance(widget, ttk.Frame):
                for child in widget.winfo_children():
                    if isinstance(child, ttk.Combobox) and child['textvariable'] == str(self.dataset_var):
                        child['values'] = list(self.xlsx_options.keys())
                        # Keep current selection if still available, otherwise select first
                        current = self.dataset_var.get()
                        if current not in self.xlsx_options:
                            if self.xlsx_options:
                                new_selection = list(self.xlsx_options.keys())[0]
                                self.dataset_var.set(new_selection)
                                self.path_var.set(self.xlsx_options[new_selection])
                        break

    def _on_dataset_change(self, event=None):
        """Handle dataset selection change"""
        selected = self.dataset_var.get()
        if selected in self.xlsx_options:
            self.path_var.set(self.xlsx_options[selected])
            # Reload data with new file
            self._show_loading("Lade neue Daten...")

            def worker():
                err = None
                df = None
                try:
                    df = self._read_excel_to_df(Path(self.path_var.get()))
                except Exception as e:
                    err = e

                def on_done():
                    if err is None and df is not None:
                        self._setup_table_from_df(df)
                    else:
                        print(f"Error loading dataset: {err}")
                    self._hide_loading()

                self.after(0, on_done)

            threading.Thread(target=worker, daemon=True).start()

    def _browse(self):
        """Browse for Excel file"""
        from tkinter import filedialog
        path = filedialog.askopenfilename(
            title="Select Ranked Excel File",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
            initialdir="Output/MCA/Ranked"
        )
        if path:
            self.path_var.set(path)
            # Reload data
            self._show_loading("Lade Datei...")

            def worker():
                err = None
                df = None
                try:
                    df = self._read_excel_to_df(Path(path))
                except Exception as e:
                    err = e

                def on_done():
                    if err is None and df is not None:
                        self._setup_table_from_df(df)
                    else:
                        print(f"Error loading file: {err}")
                    self._hide_loading()

                self.after(0, on_done)

            threading.Thread(target=worker, daemon=True).start()

    def _back_to_overview(self):
        """Close current GUI and return to launcher"""
        import subprocess
        import sys
        try:
            #
            self.destroy()

            # Start launcher
            launcher_path = Path(__file__).parent / "01_launcher.py"
            if launcher_path.exists():
                subprocess.Popen([sys.executable, str(launcher_path)], cwd=str(launcher_path.parent))
        except Exception as e:
            print(f"Error returning to launcher: {e}")
            self.destroy()

    def _build_table(self):
        wrap = ttk.Frame(self, padding=(10, 8));
        wrap.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        #
        self.category_canvas = tk.Canvas(wrap, height=20, bg="white", highlightthickness=0, bd=0)
        self.category_canvas.grid(row=0, column=0, sticky="ew", padx=0, pady=0)

        self.tree = ttk.Treeview(wrap, show="headings", height=16)
        vsb = ttk.Scrollbar(wrap, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(wrap, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscroll=vsb.set, xscroll=hsb.set)
        self.tree.grid(row=1, column=0, sticky="nsew");
        vsb.grid(row=1, column=1, sticky="ns");
        hsb.grid(row=2, column=0, sticky="ew")
        wrap.rowconfigure(1, weight=1);
        wrap.columnconfigure(0, weight=1)
        self.tree.bind("<Double-1>", self._on_row_double_click)

        hsb.config(command=self._on_h_scroll)

        footer = ttk.Frame(self, padding=(10, 8))
        footer.pack(side=tk.BOTTOM, fill=tk.X)
        ttk.Button(footer, text="Back to Overview", command=self._back_to_overview).pack(side=tk.RIGHT)

    def _setup_tree(self, cols: List[str]):
        self.tree["columns"] = cols

        def make_header(col_name):

            return col_name.replace("_", " ").title()

        headers = {
            "id": "Rank",
            "country": "Country",
            "name": "Plant name",
        }

        for c in cols:

            if c in headers:
                header_text = headers[c]
            else:
                header_text = make_header(c)
            self.tree.heading(c, text=header_text)

            w = 120
            stretch = True
            if c == "id":
                w = 50
                stretch = False
            elif c == "country":
                w = 60
                stretch = False
            elif c == "name":
                w = 280
            elif c == "uwwtd_code":
                w = 160
            elif c in ("lat", "lon"):
                w = 110
            elif c in ("mean_rank", "rank_std_dev"):
                w = 100
            elif c == "top_20_frequency":
                w = 130
            elif "score" in c:
                w = 150

            self.tree.column(c, width=w, minwidth=w, stretch=stretch, anchor=tk.W)

        self.tree.update_idletasks()
        self._draw_category_bars(cols)

    def _draw_category_bars(self, cols):

        self.category_canvas.delete("all")

        log("[DEBUG] Drawing category bars for columns:")
        for c in cols:
            color = self.SCORE_COLORS.get(c, "#ffffff")
            print(f"  Column: {c} -> Color: {color}")

        x_offset = 0
        for c in cols:
            col_width = self.tree.column(c, "width")

            color = self.SCORE_COLORS.get(c, "#ffffff")

            self.category_canvas.create_rectangle(
                x_offset, 0, x_offset + col_width, 8,
                fill=color, outline=""
            )

            x_offset += col_width

    def _on_h_scroll(self, *args):

        self.tree.xview(*args)

        cols = list(self.tree["columns"])
        self._draw_category_bars(cols)

    def _load_excel_to_table(self, path: Path):
        gdf = load_facilities(str(path))
        df = pd.DataFrame(gdf.drop(columns=gdf.geometry.name))
        df["lon"] = gdf.geometry.x;
        df["lat"] = gdf.geometry.y

        if "uwwtd_code" in df.columns:
            df["country"] = df["uwwtd_code"].astype(str).str[:2]
        else:
            df["country"] = ""

        try:

            xls = pd.ExcelFile(str(path))

            calc_sheet_name = None
            for sheet in xls.sheet_names:
                if "calculated" in sheet.lower() and "value" in sheet.lower():
                    calc_sheet_name = sheet
                    break

            if calc_sheet_name:
                calc_vals = pd.read_excel(str(path), sheet_name=calc_sheet_name)

                def norm(s):
                    import re as _re
                    return _re.sub(r"[^0-9a-zA-Z]+", "_", str(s).strip().lower()).strip("_")

                calc_vals.columns = [norm(c) for c in calc_vals.columns]

                key = None
                if "uwwtd_code" in calc_vals.columns and "uwwtd_code" in df.columns:
                    key = "uwwtd_code"
                elif "id" in calc_vals.columns and "id" in df.columns:
                    key = "id"
                elif "name" in calc_vals.columns and "name" in df.columns:
                    key = "name"

                if key:
                    exclude_cols = [key]
                    score_cols = [c for c in calc_vals.columns if c not in exclude_cols]
                    merge_cols = [key] + score_cols
                    calc_subset = calc_vals[merge_cols]

                    df = df.merge(calc_subset, on=key, how="left")
        except Exception as e:
            print(f"[INFO] Could not load Calculated Values sheet: {e}")
            pass

        for col in df.columns:
            if "score" in col.lower():
                df[col] = pd.to_numeric(df[col], errors='coerce').round(5)
            elif "rank" in col.lower():
                df[col] = pd.to_numeric(df[col], errors='coerce').round(0).astype('Int64')

        base_cols = ["id", "country", "name"]
        exclude_cols = ["uwwtd_code", "lat", "lon", "geometry", "rank", "score_ci_lower", "score_ci_upper",
                        "rank_std_dev", "rank_ci_width", "top_20_frequency"]  # exclude CI and std dev columns

        score_order = [
            "final_score",
            "h2_renewables_potential_score", "renewables_potential_score", "h2_renewables_score", "renewables_score",
            "h2_logistics_score", "logistic_score", "logistics_score",
            "grid_electricity_score",
            "district_heating_score",

            "oxygen_score",
        ]

        all_score_cols = [c for c in df.columns if
                          "score" in c.lower() and c not in base_cols and c not in exclude_cols]

        score_cols = [c for c in score_order if c in all_score_cols]
        score_cols += [c for c in all_score_cols if c not in score_cols]

        ranking_cols = [c for c in ["mean_rank"] if c in df.columns]

        other_cols = [c for c in df.columns if
                      c not in base_cols and c not in exclude_cols and c not in score_cols and c not in ranking_cols]

        display_cols = base_cols + score_cols + other_cols + ranking_cols

        self.df = df.copy()
        self.display_cols = display_cols
        self._setup_tree(display_cols)
        for i in self.tree.get_children(): self.tree.delete(i)
        for _, r in self.df.iterrows():
            self.tree.insert("", tk.END, values=[r[c] if c in r else "" for c in display_cols])

    def _on_row_double_click(self, _e):
        try:
            item_id = self.tree.focus()
            if not item_id: return
            vals = self.tree.item(item_id, "values")
            if not vals: return

            all_items = self.tree.get_children()
            row_idx = all_items.index(item_id)

            df_row = self.df.iloc[row_idx]

            props = {
                "id": df_row.get("id"),
                "uwwtd_code": df_row.get("uwwtd_code"),
                "name": df_row.get("name"),
                "lat": float(df_row.get("lat")) if df_row.get("lat") not in (None, "", "None") and pd.notna(
                    df_row.get("lat")) else None,
                "lon": float(df_row.get("lon")) if df_row.get("lon") not in (None, "", "None") and pd.notna(
                    df_row.get("lon")) else None,
                "capacity": df_row.get("capacity"),
                "ozonation": df_row.get("ozonation"),
            }

            key = props.get("uwwtd_code") or props.get("id")
            if key is not None:
                tech = getattr(self, "_tech_by_key", {}).get(str(key), {})
                if tech:
                    if "capacity_pe" in tech:
                        props["capacity_pe"] = tech["capacity_pe"]
                    if "ozonation" in tech:
                        props["ozonation"] = tech["ozonation"]
            SteckbriefDialog(self, props)
        except Exception as e:
            print(f"[ERROR] Failed to open plant details: {e}")
            import traceback
            traceback.print_exc()
            from tkinter import messagebox
            messagebox.showerror("Error", f"Failed to open plant details:\n{str(e)}")


class SteckbriefDialog(tk.Toplevel):

    def _show_map_loading(self, message: str = "Loading WWTP.."):
        if getattr(self, "_map_overlay", None):
            return
        # Overlay NUR über dem Kartenbereich anzeigen
        parent = getattr(self, "_map_frame", self)
        self._map_overlay = tk.Frame(parent, bg="#ffffff")
        self._map_overlay.place(relx=0, rely=0, relwidth=1, relheight=1)
        self._map_overlay.lift()

        self._map_label_loading = ttk.Label(self._map_overlay, text=message)
        self._map_label_loading.place(relx=0.5, rely=0.5, anchor="s")

        self._map_bar = ttk.Progressbar(self._map_overlay, mode="indeterminate", length=160)
        self._map_bar.place(relx=0.5, rely=0.5, anchor="n", y=6)
        try:
            self._map_bar.start(10)
        except Exception:
            pass

    def _hide_map_loading(self):
        if getattr(self, "_map_overlay", None):
            try:
                if getattr(self, "_map_bar", None):
                    self._map_bar.stop()
            except Exception:
                pass
            self._map_overlay.destroy()
            self._map_overlay = None
            self._map_bar = None
            self._map_label_loading = None

    def _load_map_async(self, center_lat, center_lon, overlay_poly):
        self._show_map_loading()

        def worker():
            base_img = provider = overlay_img = None
            try:
                base_img, provider = fetch_esri_image(center_lat, center_lon, None)
                overlay_img, _ = fetch_esri_image(center_lat, center_lon, overlay_poly)
            except Exception:
                pass

            def on_done():
                if base_img is None:
                    self._map_label.configure(text="Could not fetch aerial imagery.")
                else:
                    self._img_base_pil = base_img
                    self._img_overlay_pil = overlay_img
                    self._img_base = ImageTk.PhotoImage(self._img_base_pil)
                    self._img_overlay = ImageTk.PhotoImage(self._img_overlay_pil) if overlay_img else None
                    self._map_label.configure(image=self._img_base)
                self._hide_map_loading()

            self.after(0, on_done)

        threading.Thread(target=worker, daemon=True).start()

    def __init__(self, master: App, props: Dict[str, Any]):
        super().__init__(master)
        self.title(f"{props.get('uwwtd_code', 'UWWTD n/a')} – {props.get('name', 'Plant')}")
        self._plant_name = props.get('name') or props.get('Name') or ''
        self.withdraw()
        self.update_idletasks()
        sw = self.winfo_screenwidth();
        sh = self.winfo_screenheight()
        x = max((sw - 1100) // 2, 0);
        y = max((sh - 780) // 2, 0)
        self.geometry(f"1100x780+{x}+{y}")
        self.resizable(False, False)

        self.deiconify()
        header = ttk.Frame(self, padding=(12, 10));
        header.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(header, text=f"{props.get('uwwtd_code', 'UWWTD n/a')} – {props.get('name', '')}",
                  font=("Segoe UI", 12, "bold")).pack(side=tk.LEFT)

        top = ttk.Frame(self, padding=(12, 6))
        top.pack(side=tk.TOP, fill=tk.X, expand=False)

        left = ttk.LabelFrame(top, text="Aerial view with geodata")
        right = ttk.LabelFrame(top, text="General information")
        left.grid(row=0, column=0, sticky="nw", padx=(0, 6))
        right.grid(row=0, column=1, sticky="ne", padx=(6, 0))

        # feste Geometrien
        left.configure(width=DISPLAY_W + 2 * MAP_PAD, height=DISPLAY_H + 2 * MAP_PAD)
        right.configure(width=SIDEPANEL_W)
        left.grid_propagate(False)
        right.grid_propagate(False)

        # fester Kartenausschnitt-Container
        self._map_frame = tk.Frame(left, width=DISPLAY_W, height=DISPLAY_H, bd=0, highlightthickness=0)
        self._map_frame.pack(padx=MAP_PAD, pady=MAP_PAD)
        self._map_frame.pack_propagate(False)

        # Bild-Label füllt den Kartenausschnitt exakt
        self._map_label = ttk.Label(self._map_frame)
        self._map_label.place(relx=0, rely=0, relwidth=1, relheight=1)

        # Zustände für Dropdown
        self._overlay_visible = False
        self._pipeline_visible = False
        self._free_area_visible = False
        # kleines "Layers"-Icon oben links + Dropdown
        self._layers_icon = self._make_layers_icon(22, 22)
        self._menu_btn = ttk.Button(self._map_frame, image=self._layers_icon, command=self._open_map_menu)
        self._menu_btn.place(relx=1.0, rely=0.0, x=-8, y=8, anchor="ne")
        self._overlay_visible = False
        overlay_poly = None
        center_lat = None
        center_lon = None
        if props.get("lat") is not None and props.get("lon") is not None:
            nearest = find_nearest_shape(props["lon"], props["lat"])
            if nearest is not None:
                c = nearest.centroid
                center_lon, center_lat = c.x, c.y
                overlay_poly = nearest
            else:
                center_lat, center_lon = props["lat"], props["lon"]

        self._center_lat, self._center_lon = center_lat, center_lon
        self._props_lat, self._props_lon = props.get("lat"), props.get("lon")
        if center_lat is not None and center_lon is not None:
            self._load_map_async(center_lat, center_lon, overlay_poly)

        # General info

        info = [("Rank", props.get("id")), ("Name", props.get("name"))]
        if props.get("lat") and props.get("lon"):
            info.append(("Coordinates", f"{props['lat']:.5f}, {props['lon']:.5f}"))

        if props.get("capacity_pe") is not None:
            info.append(("Capacity/PE", str(props.get("capacity_pe"))))
        oz_val = props.get("ozonation")
        if oz_val is not None:
            _s = str(oz_val).strip().lower()
            # normalize 1/0/true/false/yes/no/ja/nein to Yes/No
            if _s in ("1", "true", "yes", "y", "ja"):
                _oz = "Yes"
            elif _s in ("0", "false", "no", "n", "nein"):
                _oz = "No"
            else:
                try:
                    _oz = "Yes" if float(_s) != 0.0 else "No"
                except Exception:
                    _oz = str(oz_val)
            info.append(("Ozonation", _oz))
        # Capacity und Ozonation
        if props.get("capacity") is not None:
            info.append(("Capacity", str(props.get("capacity"))))
        oz_val = props.get("ozonation")
        if oz_val is not None:
            _s = str(oz_val).strip().lower()
            if _s in ("1", "true", "yes", "y", "ja"):
                _oz = "Yes"
            elif _s in ("0", "false", "no", "n", "nein"):
                _oz = "No"
            else:
                _oz = str(oz_val)
            info.append(("Ozonation", _oz))

        _dedup = []
        _seen = set()
        for _k, _v in info:
            if _k not in _seen and _v is not None:
                _dedup.append((_k, _v))
                _seen.add(_k)
        info = _dedup

        for k, v in info:
            row = ttk.Frame(right);
            row.pack(anchor="w", fill=tk.X, padx=8, pady=4)
            ttk.Label(row, text=f"{k}:", width=24, foreground="#6c757d").pack(side=tk.LEFT)
            ttk.Label(row, text=str(v)).pack(side=tk.LEFT)

        # KPIs (dummy)
        kpi_box = ttk.LabelFrame(self, text="KPIs");
        kpi_box.pack(side=tk.TOP, fill=tk.X, padx=12, pady=(4, 6))
        ttk.Label(kpi_box, text="No KPIs found.").pack(anchor="w", padx=8, pady=6)

        # Derived (dummy)
        end_box = ttk.LabelFrame(self, text="Derived results");
        end_box.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=12, pady=(0, 12))
        demo = {"Nitrate (mg/L)": 7.2, "Phosphate (mg/L)": 0.8, "Daily average flow (m³/d)": 15200,
                "Last update": "2025-10-31"}
        ttk.Label(end_box, text="No results found.").pack(anchor="w", padx=8, pady=6)
        ttk.Frame(self).pack(side=tk.BOTTOM, fill=tk.X, padx=12, pady=(0, 12))
        ttk.Button(self, text="Close", command=self.destroy).pack(side=tk.RIGHT, padx=16, pady=8)

    def _update_map_image(self):
        try:
            img = self._img_overlay_pil if getattr(self, "_overlay_visible", False) else self._img_base_pil
        except AttributeError:
            return
        if img is None:
            return
        self._imgtk = ImageTk.PhotoImage(img)
        self._map_label.configure(image=self._imgtk)

    # ---- Dropdown & Icon ----

    def _open_esri_map(self):
        """Lokalen MapLibre-Viewer öffnen (nur MapLibre, ohne Leaflet)."""
        # --- mehrfaches Öffnen verhindern ---
        if getattr(self, "_map_opening_now", False):
            return
        setattr(self, "_map_opening_now", True)

        from pathlib import Path
        import html as _html, json as _json

        # Zentrum / Anlagenname
        lon = getattr(self, "_center_lon", None) or getattr(self, "_props_lon", None)
        lat = getattr(self, "_center_lat", None) or getattr(self, "_props_lat", None)
        plant_nm = (getattr(self, "_plant_name", "") or getattr(self, "plant_name", "") or "")

        payloads = {}

        # Nur MapLibre-spezifische Bibliotheken laden
        _fetch_maplibre_libs(payloads)

        # WWTP Icon Bild hinzufügen
        try:
            import base64
            icon_path = Path("Daten") / "Pictures" / "icons8-water-treatment-plant-100.png"
            if icon_path.exists():
                with open(icon_path, 'rb') as f:
                    icon_data = f.read()
                payloads["/wwtp-icon.png"] = icon_data
                print("[ICON] WWTP icon image loaded")
            else:
                print("[ICON] WWTP icon image not found at", icon_path)
        except Exception as e:
            print("[ICON] Failed to load WWTP icon:", e)

        try:
            icon_dir = Path("Daten") / "overlays_512px"
            mapping = {
                "/icons/plant-waste.png": "Abfall.png",
                "/icons/plant-nuclear.png": "atomkraft.png",
                "/icons/plant-battery.png": "batterie.png",
                "/icons/plant-biomass.png": "biomasse.png",
                "/icons/tower-hv.png": "Strommast.png",
                "/icons/tower-hv-transition.png": "Strommast.png",
                "/icons/plant-oilgas.png": "gas und öl.png",
                "/icons/plant-geothermal.png": "geothermie.png",
                "/icons/plant-coal.png": "kohle.png",
                "/icons/plant-solar.png": "solar.png",
                "/icons/plant-hydro.png": "wasserkraft.png",
                "/icons/plant-wind.png": "windenergie.png",
                "/icons/plant-generic.png": "solar.png",
                "/icons/free-area.png": "FreeArea.png"
            }
            for route, fname in mapping.items():
                fpath = icon_dir / fname
                if fpath.exists():
                    with open(fpath, "rb") as f:
                        payloads[route] = f.read()
                        print("[ICON] loaded", route, "from", fpath)
                else:
                    print("[ICON] missing", fpath)
        except Exception as e:
            print("[ICON] Failed to load custom PNG icons:", e)

        # --- Daten einsammeln  ---
        try:
            outlines = _gpkg_to_geojson_str(GPKG_PATH)
            if outlines:
                payloads["/outlines.geojson"] = outlines
        except Exception:
            pass

        if lat is not None and lon is not None:
            try:
                poly = find_nearest_shape(lon, lat)
                if poly is not None:
                    import geopandas as gpd
                    payloads["/plant.geojson"] = gpd.GeoDataFrame({"id": [1]}, geometry=[poly],
                                                                  crs="EPSG:4326").to_json()
            except Exception:
                pass
            try:
                fa = _gpkg_polygons_within_radius(FREE_AREA_GPKG, lon, lat, 1000)
                if fa:
                    payloads["/freearea.geojson"] = fa
            except Exception:
                pass

        try:
            from pathlib import Path as _P
            for _gpkg in PIPELINE_GPKGS:  # routes_ehb / routes_gaspipelines
                try:
                    _name = _P(_gpkg).stem
                    _geo = _merge_and_clip_pipelines([_gpkg], lon, lat, 1000)  # 1 km Umkreis
                    if _geo:
                        payloads[f"/{_name}_clip.geojson"] = _geo
                except Exception:
                    pass
        except Exception:
            pass

        try:
            gas_all = _gpkg_to_geojson_str(GAS_MAIN_GPKG)
            if gas_all:
                payloads["/gasmains.geojson"] = gas_all
        except Exception:
            pass
        try:
            p = Path(JSON_HP_PATH)
            if p.exists():
                payloads["/ehb_hp.json"] = p.read_text(encoding="utf-8")
        except Exception:
            pass
        try:
            p = Path(JSON_TRANSMISSION_PATH)
            if p.exists():
                payloads["/ehb_transmission.json"] = p.read_text(encoding="utf-8")
        except Exception:
            pass

        # District Heating Networks (20 km radius)
        if lat is not None and lon is not None:
            try:
                print(f"[DH] Loading district heating for lat={lat}, lon={lon}")
                print(f"[DH] File path: {DISTRICT_HEATING_GEOJSON}")
                print(f"[DH] File exists: {Path(DISTRICT_HEATING_GEOJSON).exists()}")
                dh = _geojson_clip_radius(DISTRICT_HEATING_GEOJSON, lon, lat, 20000)
                if dh:
                    payloads["/district_heating.geojson"] = dh
                    print(f"[DH] ✓ District heating data loaded, size: {len(dh)} bytes")
                else:
                    print("[DH] No district heating data found within 10 km")
            except Exception as e:
                print(f"[DH] Error loading district heating: {e}")
                import traceback
                traceback.print_exc()

        # --- HTML aufbauen ---
        page_title = f'WWTP Map: "{plant_nm}"' if plant_nm else "WWTP Map"
        maplibre_html = [
            "<!doctype html><html><head><meta charset='utf-8'>",
            "<meta name='viewport' content='width=device-width,initial-scale=1'>",
            f"<title>{_html.escape(page_title, quote=True)}</title>",
            "<link rel='stylesheet' href='/lib/maplibre-gl.css'/>",
            "<script src='/lib/maplibre-gl.js'></script>",
            "<style>",
            "html,body,#map{height:100%;margin:0}",
            ".maplibre-control-layers{background:white;padding:10px;min-width:250px;max-height:60vh;overflow:auto;font-family:Arial,sans-serif;font-size:12px;}",
            ".grp{margin:6px 0 0 0;}.grp-h{display:block;margin:6px 0 4px 2px;font-weight:700;color:#111827;}.grp-items{}",
            ".grp-sep{height:1px;background:#e5e7eb;margin:6px 0;}",
            "</style>",
            "</head><body><div id='map'></div>",
            "<script>",
            "// Enhanced error handling and debugging",
            "window.addEventListener('error', function(e){ console.error('JS Error:', e.message, 'at', e.filename, ':', e.lineno); });",
            "window.addEventListener('unhandledrejection', function(e){ console.error('Unhandled Promise:', e.reason); });",
            "console.log('[BOOT] Starting MapLibre map initialization...');",
            "console.log('[CHECK] MapLibre available:', typeof maplibregl !== 'undefined');",
            f"document.title={_json.dumps(page_title, ensure_ascii=False)};",
        ]
        # MapLibre map initialization
        if lat is not None and lon is not None:
            maplibre_html.append(f"""
try {{
    console.log('[MAPLIBRE] Initializing MapLibre map at [{lat:.6f}, {lon:.6f}]');

    // Speichert WWTP Koodinaten und Name
    var wwtpLat = {lat:.6f};
    var wwtpLon = {lon:.6f};
    var wwtpPlantName = {_json.dumps(plant_nm or "WWTP")};

    var map = new maplibregl.Map({{
        container: 'map',
        center: [{lon:.6f}, {lat:.6f}],
        zoom: 16,
        style: {{
            version: 8,

            sources: {{}},
            layers: []
        }}
    }});

    console.log('[MAPLIBRE] Map created successfully');
}} catch(e) {{
    console.error('[MAPLIBRE] Failed to create map:', e);
    document.body.innerHTML = '<h1>MapLibre initialization failed: ' + e.message + '</h1>';
}}""")
        else:
            maplibre_html.append("""
try {
    console.log('[MAPLIBRE] Initializing default MapLibre map view');

    var map = new maplibregl.Map({
        container: 'map',
        center: [14.0, 48.0],
        zoom: 6,
        style: {
            version: 8,

            sources: {},
            layers: []
        }
    });

    console.log('[MAPLIBRE] Map created successfully');
} catch(e) {
    console.error('[MAPLIBRE] Failed to create map:', e);
    document.body.innerHTML = '<h1>MapLibre initialization failed: ' + e.message + '</h1>';
}""")

        maplibre_html += [
            """
// Wartet aufs Laden von MapLibre
map.on('load', function() {
    console.log('[MAPLIBRE] Map loaded, adding all layers...');

    // Läd Power icons und tausch die Standardicons zu den Neuen
    map.loadImage('/icons/tower-hv.png', (err, image) => {
        if (!err && image) {
            try {
                map.addImage('tower-hv', image, { pixelRatio: 1 });
                console.log('[ICON] Successfully loaded tower icon');

                // Wechselt die Power-Layer auf die neuen Icons
                setTimeout(() => {
                    try {
                        if (map.getLayer('oim-power_tower')) {
                            // Remove the circle layer
                            map.removeLayer('oim-power_tower');

                            // Addet Sybollayer mit den neuen Icons
                            map.addLayer({
                                id: 'oim-power_tower',
                                type: 'symbol',
                                source: 'oim-energy',
                                'source-layer': 'power_tower',
                                layout: {
                                    'visibility': 'visible',
                                    'icon-image': 'tower-hv',
                                    'icon-size': ['interpolate', ['linear'], ['zoom'], 8, 0.12, 12, 0.18, 16, 0.24],
                                    'icon-anchor': 'center',
                                    'icon-allow-overlap': true
                                }
                            });
                            console.log('[ICON] Switched power_tower layer to use icon');
                        }
                    } catch(e) {
                        console.error('[ICON] Failed to switch layer to icon:', e);
                    }
                }, 2000); // Wait 2 seconds for layers to be ready

            } catch(e) {
                console.error('[ICON] Failed to add tower icon:', e);
            }
        } else {
            console.error('[ICON] Failed to load tower icon:', err);
        }
    });

    try {
        // Addet Esri World Imagery als basemap
        map.addSource('esri-imagery', {
            type: 'raster',
            tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
            tileSize: 256,
            attribution: '© Esri'
        });

        map.addLayer({
            id: 'esri-imagery-layer',
            type: 'raster',
            source: 'esri-imagery'
        });

        // Addet OSM 
        map.addSource('osm', {
            type: 'raster',
            tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
            tileSize: 256,
            attribution: '© OpenStreetMap'
        });

        map.addLayer({
            id: 'osm-layer',
            type: 'raster',
            source: 'osm',
            layout: { visibility: 'none' }
        });

        // Addet Esri Reference layer
        map.addSource('esri-reference', {
            type: 'raster',
            tiles: ['https://services.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}'],
            tileSize: 256,
            attribution: '© Esri'
        });

        map.addLayer({
            id: 'esri-reference-layer',
            type: 'raster',
            source: 'esri-reference',
            layout: { visibility: 'visible' }
        });

        console.log('[MAPLIBRE] Base layers added successfully');

        // Addet eine simple layer control mit Gruppen
        addSimpleLayerControl();

        // Addet alle Layer
        addAllOverlayLayers();

        // Addet District Heating Networks
        // Wartet auzf Vervolständigung
        addDistrictHeatingLayer().then(() => {
            // Add OpenInfraMap Energy (will render on top)
            addEnergyLayer();
        }).catch(() => {
            // fährt bei Fehler fort
            addEnergyLayer();
        });

        // Addet WWTP Icon und anlagen name
        if (typeof wwtpLat !== 'undefined' && typeof wwtpLon !== 'undefined') {

            var plantName = (typeof wwtpPlantName !== 'undefined') ? wwtpPlantName : 'WWTP';
            addWWTPIcon(wwtpLat, wwtpLon, plantName);
        }

    } catch(e) {
        console.error('[MAPLIBRE] Failed to add layers:', e);
    }
});



// Simple Funktionen für das MapLibre layer management
function addAllOverlayLayers() {
    console.log('[LAYERS] Adding all overlay layers...');


    if (typeof addDataLayers === 'function') {
        addDataLayers();
    }

    console.log('[LAYERS] ✓ All overlay layers added');
}

function addDataLayers() {
    // Addet GeoJSON layer wenn verfügbar
    fetch('/outlines.geojson')
        .then(response => response.ok ? response.json() : null)
        .then(data => {
            if (data) {
                map.addSource('outlines', { type: 'geojson', data: data });
                map.addLayer({
                    id: 'outlines-layer',
                    type: 'line',
                    source: 'outlines',
                    paint: {
                        'line-color': '#1d4ed8',
                        'line-width': 2,
                        'line-opacity': 0.8
                    },
                    layout: { visibility: 'none' }
                });
                addOverlayControl('WWTP Outlines', ['outlines-layer']);
            }
        })
        .catch(e => console.log('[DATA] No outlines data available'));

    fetch('/plant.geojson')
        .then(response => response.ok ? response.json() : null)
        .then(data => {
            if (data) {
                map.addSource('plant', { type: 'geojson', data: data });
                map.addLayer({
                    id: 'plant-fill',
                    type: 'fill',
                    source: 'plant',
                    paint: {
                        'fill-color': '#3b82f6',
                        'fill-opacity': 0.3
                    }
                });
                map.addLayer({
                    id: 'plant-outline',
                    type: 'line',
                    source: 'plant',
                    paint: {
                        'line-color': '#1d4ed8',
                        'line-width': 2
                    }
                });
                addOverlayControl('Plant Area', ['plant-fill', 'plant-outline'], true);

                // Fitten auf Anlagengrenzen
                const bounds = new maplibregl.LngLatBounds();
                data.features.forEach(feature => {
                    if (feature.geometry.type === 'Polygon') {
                        feature.geometry.coordinates[0].forEach(coord => {
                            bounds.extend(coord);
                        });
                    }
                });
                if (!bounds.isEmpty()) {
                    map.fitBounds(bounds, { padding: 50 });
                }
            }
        })
        .catch(e => console.log('[DATA] No plant data available'));
}

function addEnergyLayer() {
    console.log('[ENERGY] Adding OpenInfraMap energy layer...');

    try {
        // Addet OpenInfraMap vector tiles Quelle
        map.addSource('oim-energy', {
            type: 'vector',
            tiles: ['https://openinframap.org/tiles/{z}/{x}/{y}.pbf'],
            minzoom: 0,
            maxzoom: 17,
            attribution: '© <a href="https://openinframap.org">OpenInfraMap</a>'
        });



        console.log('[OIM] Sources added');
        // Registert PNG icons 
        const ICON_ROUTES = {
            'plant-waste': '/icons/plant-waste.png',
            'plant-nuclear': '/icons/plant-nuclear.png',
            'plant-battery': '/icons/plant-battery.png',
            'plant-biomass': '/icons/plant-biomass.png',
            'plant-oilgas': '/icons/plant-oilgas.png',
            'plant-geothermal': '/icons/plant-geothermal.png',
            'plant-coal': '/icons/plant-coal.png',
            'plant-solar': '/icons/plant-solar.png',
            'plant-hydro': '/icons/plant-hydro.png',
            'plant-wind': '/icons/plant-wind.png',
            'plant-generic': '/icons/plant-generic.png',
            'tower-hv': '/icons/tower-hv.png',
            'tower-hv-transition': '/icons/tower-hv-transition.png',
            'free-area': '/icons/free-area.png'
        };
        const _requestedIcons = new Set();
        map.on('styleimagemissing', (e) => {
            const id = e.id;
            const url = ICON_ROUTES[id];
            if (!url || _requestedIcons.has(id)) return;
            _requestedIcons.add(id);
            map.loadImage(url, (err, image) => {
                if (!err && image) {
                    try { 
                        map.addImage(id, image, { pixelRatio: 1 }); 
                        console.log('[ICON] Successfully loaded icon:', id, 'from', url);
                    } catch(e){ 
                        console.error('[ICON] Failed to add image:', id, e);
                    }
                } else {
                    console.error('[ICON] Failed to load image:', id, 'from', url, err);
                }
            });
        });




        console.log('[OIM DEBUG] Adding all known OIM layers...');

        // Liste aller bekannten OIM Layer
        const knownOIMSourceLayers = [
            'power_plant_point',
            'power_generator', 
            'power_tower',
            'power_transformer',
            'power_substation_point',
            'power_switch',
            'power_compensator',
            'power_converter',

        ];

        // Addet alle bekannten layer
        knownOIMSourceLayers.forEach(sourceLayer => {
            try {
                const layerId = `oim-${sourceLayer}`;
                console.log('[OIM DEBUG] Adding layer for:', sourceLayer);

                // Bestimmt ein passendes Sytling

let layerConfig = {
                    id: layerId,
                    source: 'oim-energy',
                    'source-layer': sourceLayer,
                    layout: { visibility: 'visible' }
                };


                if (sourceLayer.includes('cable') || sourceLayer.includes('line')) {
                    // Skip - we'll handle lines separately below
                    return;
                } else if (sourceLayer === 'power_plant_point') {
                    // Skip - we'll handle power plants with separate layers below
                    return;

                } else if (sourceLayer === 'power_tower') {
                    layerConfig.type = 'circle';
                    layerConfig.paint = {
                        'circle-color': '#6b7280',
                        'circle-radius': [
                            'interpolate', ['linear'], ['zoom'],
                            10, 2,
                            14, 3,
                            18, 5
                        ],
                        'circle-opacity': 0.7
                    };


                } else {
                    // Fall-Back
                    layerConfig.type = 'circle';
                    layerConfig.paint = {
                        'circle-color': '#6b7280',
                        'circle-radius': ['interpolate', ['linear'], ['zoom'], 8, 3, 12, 6, 16, 10],
                        'circle-opacity': 0.8,
                        'circle-stroke-color': '#ffffff',
                        'circle-stroke-width': 1
                    };
                }

                map.addLayer(layerConfig);

                console.log('[OIM DEBUG] Successfully added layer:', layerId);

            } catch (error) {
                console.log('[OIM DEBUG] Error adding layer', sourceLayer, ':', error);
            }
        });


// Addet substations filler
        map.addLayer({
            id: 'power-substations-fill',
            type: 'fill',
            source: 'oim-energy',
            'source-layer': 'power_substation',
            paint: {
                'fill-color': '#3b82f6',
                'fill-opacity': 0.2
            },
            layout: { visibility: 'none' }
        });

        // Addet substations outline
        map.addLayer({
            id: 'power-substations-outline',
            type: 'line',
            source: 'oim-energy',
            'source-layer': 'power_substation',
            paint: {
                'line-color': '#1d4ed8',
                'line-width': 1.5,
                'line-opacity': 0.8
            },
            layout: { visibility: 'none' }
        });

        // Addet power plants von den OIM tiles
        map.addLayer({
            id: 'power-plants-oim',
            type: 'circle',
            source: 'oim-energy',
            'source-layer': 'power_plant_point',
            paint: {
                'circle-color': [
                    'case',
                    ['==', ['get', 'source'], 'nuclear'], '#dc2626',
                    ['==', ['get', 'source'], 'coal'], '#374151',
                    ['==', ['get', 'source'], 'gas'], '#f59e0b',
                    ['==', ['get', 'source'], 'oil'], '#7c2d12',
                    ['==', ['get', 'source'], 'hydro'], '#0ea5e9',
                    ['==', ['get', 'source'], 'wind'], '#10b981',
                    ['==', ['get', 'source'], 'solar'], '#eab308',
                    ['==', ['get', 'source'], 'biomass'], '#65a30d',
                    ['==', ['get', 'source'], 'geothermal'], '#dc2626',
                    '#f59e0b'
                ],
                'circle-radius': [
                    'interpolate', ['linear'], ['zoom'],
                    5, 4,
                    8, 6,
                    12, 10,
                    16, 15
                ],
                'circle-opacity': 0.8,
                'circle-stroke-color': '#ffffff',
                'circle-stroke-width': 2
            },
            layout: { visibility: 'none' }
        });

        // Addet power generators von den OIM vector tiles
        map.addLayer({
            id: 'power-generators-oim',
            type: 'circle',
            source: 'oim-energy',
            'source-layer': 'power_generator',
            paint: {
                'circle-color': [
                    'case',
                    ['==', ['get', 'source'], 'wind'], '#10b981',
                    ['==', ['get', 'source'], 'solar'], '#eab308',
                    ['==', ['get', 'source'], 'hydro'], '#0ea5e9',
                    '#8b5cf6'
                ],
                'circle-radius': [
                    'interpolate', ['linear'], ['zoom'],
                    8, 3,
                    12, 5,
                    16, 8
                ],
                'circle-opacity': 0.8,
                'circle-stroke-color': '#ffffff',
                'circle-stroke-width': 1
            },
            layout: { visibility: 'none' }
        });

        // Addet transformers von den OIM vector tiles
        map.addLayer({
            id: 'power-transformers-oim',
            type: 'circle',
            source: 'oim-energy',
            'source-layer': 'power_transformer',
            paint: {
                'circle-color': '#f59e0b',
                'circle-radius': [
                    'interpolate', ['linear'], ['zoom'],
                    10, 3,
                    14, 5,
                    18, 8
                ],
                'circle-opacity': 0.8,
                'circle-stroke-color': '#ffffff',
                'circle-stroke-width': 1
            },
            layout: { visibility: 'none' }
        });

        // Addet substation punkte von den OIM vector tiles
        map.addLayer({
            id: 'power-substation-points-oim',
            type: 'circle',
            source: 'oim-energy',
            'source-layer': 'power_substation_point',
            paint: {
                'circle-color': '#3b82f6',
                'circle-radius': [
                    'interpolate', ['linear'], ['zoom'],
                    8, 4,
                    12, 6,
                    16, 10
                ],
                'circle-opacity': 0.8,
                'circle-stroke-color': '#ffffff',
                'circle-stroke-width': 2
            },
            layout: { visibility: 'none' }
        });

        // =====  VOLTAGE-BASED POWER LINES =====

        // Addet power lines mit auf SPannung basierten Farben
        map.addLayer({
            id: 'power-lines-voltage',
            type: 'line',
            source: 'oim-energy',
            'source-layer': 'power_line',
            paint: {
                'line-color': [
                    'case',
                    // HGÜ (DC) -> violett
                    ['any',
                        ['==', ['get', 'dc'], 'yes'],
                        ['==', ['get', 'hvdc'], 'yes'],
                        ['==', ['to-number', ['coalesce', ['get', 'frequency'], 50]], 0]
                    ], '#800080',
                    // Traktion < 50 Hz -> hellgrau
                    ['<', ['to-number', ['coalesce', ['get', 'frequency'], 50]], 50], '#a0a0a0',
                    // Voltage-based colors (OpenInfraMap legend) - voltage is in kV!
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 550], '#00cccc',  // ≥ 550 kV - Cyan
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 310], '#cc00cc',  // ≥ 310 kV - Purple
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 220], '#cc0000',  // ≥ 220 kV - Red
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 132], '#cc6600',  // ≥ 132 kV - Orange
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 52], '#cccc00',   // ≥ 52 kV - Yellow
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 25], '#00cc00',   // ≥ 25 kV - Green
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 10], '#6699ff',   // ≥ 10 kV - Blue
                    '#808080' // < 10kV or no voltage - Gray
                ],
                'line-width': [
                    'interpolate', ['linear'], ['zoom'],
                    6, ['case', 
                        ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 220000], 2.0,  // High voltage - thick
                        ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 52000], 1.5,   // Medium voltage
                        1.0  // Low voltage - thin
                    ],
                    12, ['case', 
                        ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 220000], 3.0,
                        ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 52000], 2.0,
                        1.5
                    ],
                    16, ['case', 
                        ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 220000], 4.0,
                        ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 52000], 3.0,
                        2.0
                    ]
                ],
                'line-opacity': 0.9
            },
            layout: { visibility: 'visible' },
            minzoom: 8  // Main power lines visible from zoom level 8+
        });


        // Adder power cables (ntergrud) mit voltage-based coloring - mit gestricherlter Linie für Unterirdisch
        // Background layer (lighter/transparent)
        map.addLayer({
            id: 'power-cables-voltage-bg',
            type: 'line',
            source: 'oim-energy',
            'source-layer': 'power_cable',
            layout: { 
                'visibility': 'visible',
                'line-cap': 'round',
                'line-join': 'round'
            },
            paint: {
                'line-color': [
                    'case',
                    // Voltage-basierte Farben 
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 550], '#00cccc',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 310], '#cc00cc',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 220], '#cc0000',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 132], '#cc6600',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 52], '#cccc00',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 25], '#00cc00',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 10], '#6699ff',
                    '#808080'
                ],
                'line-width': [
                    'interpolate', ['linear'], ['zoom'],
                    6, 1.2,
                    12, 2.4,
                    16, 3.6
                ],
                'line-opacity': 0.3,
                'line-gap-width': 0
            },
            minzoom: 10
        });

        // Vodergrund  
        map.addLayer({
            id: 'power-cables-voltage',
            type: 'line',
            source: 'oim-energy',
            'source-layer': 'power_cable',
            layout: { 
                'visibility': 'visible',
                'line-cap': 'butt',
                'line-join': 'round'
            },
            paint: {
                'line-color': [
                    'case',
                    // Voltage-basierte Farben 
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 550], '#00cccc',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 310], '#cc00cc',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 220], '#cc0000',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 132], '#cc6600',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 52], '#cccc00',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 25], '#00cc00',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 10], '#6699ff',
                    '#808080'
                ],
                'line-width': [
                    'interpolate', ['linear'], ['zoom'],
                    6, 1.0,
                    12, 2.0,
                    16, 3.0
                ],
                'line-opacity': 1.0,
                'line-dasharray': [4, 3]
            },
            minzoom: 10
        });

        // Addet minor power lines mit voltage-basierten coloring
        map.addLayer({
            id: 'power-minor-lines-voltage',
            type: 'line',
            source: 'oim-energy',
            'source-layer': 'power_minor_line',
            paint: {
                'line-color': [
                    'case',
                    // Voltage-based colors (same scheme) - voltage is in kV!
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 52], '#cccc00',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 25], '#00cc00',
                    ['>=', ['to-number', ['coalesce', ['get', 'voltage'], 0]], 10], '#6699ff',
                    '#808080'
                ],
                'line-width': [
                    'interpolate', ['linear'], ['zoom'],
                    6, 0.8,
                    12, 1.5,
                    16, 2.5
                ],
                'line-opacity': 0.8
            },
            layout: { visibility: 'visible' },
            minzoom: 12  // Minor lines only visible from zoom level 12+
        });



        // ===== POWER PLANTS mit Icons ud Kreisen =====

        // Adder power plant icons für Anlagen mit bekannter QUelle
        map.addLayer({
            id: 'power-plants-icons',
            type: 'symbol',
            source: 'oim-energy',
            'source-layer': 'power_plant_point',
            filter: [
                'any',
                ['>=', ['index-of', 'coal', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'geothermal', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'hydro', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'water', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'nuclear', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'oil', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'gas', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'solar', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'photovoltaic', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'wind', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'biomass', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'waste', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                ['>=', ['index-of', 'battery', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0]
            ],
            layout: {
                'visibility': 'visible',
                'icon-image': [
                    'let', 'src',
                    ['downcase', ['coalesce',
                        ['get', 'plant:source'],
                        ['get', 'generator:source'],
                        ['get', 'source'],
                        ['get', 'plant:method'],
                        ['get', 'generator:method'],
                        ''
                    ]],
                    ['case',
                        ['>=', ['index-of', 'coal', ['var', 'src']], 0], 'plant-coal',
                        ['>=', ['index-of', 'geothermal', ['var', 'src']], 0], 'plant-geothermal',
                        ['>=', ['index-of', 'hydro', ['var', 'src']], 0], 'plant-hydro',
                        ['>=', ['index-of', 'water', ['var', 'src']], 0], 'plant-hydro',
                        ['>=', ['index-of', 'nuclear', ['var', 'src']], 0], 'plant-nuclear',
                        ['any',
                            ['>=', ['index-of', 'oil', ['var', 'src']], 0],
                            ['>=', ['index-of', 'gas', ['var', 'src']], 0]
                        ], 'plant-oilgas',
                        ['>=', ['index-of', 'solar', ['var', 'src']], 0], 'plant-solar',
                        ['>=', ['index-of', 'photovoltaic', ['var', 'src']], 0], 'plant-solar',
                        ['>=', ['index-of', 'wind', ['var', 'src']], 0], 'plant-wind',
                        ['>=', ['index-of', 'biomass', ['var', 'src']], 0], 'plant-biomass',
                        ['>=', ['index-of', 'waste', ['var', 'src']], 0], 'plant-waste',
                        ['>=', ['index-of', 'battery', ['var', 'src']], 0], 'plant-battery',
                        'plant-generic'
                    ]
                ],
                'icon-size': ['interpolate', ['linear'], ['zoom'], 6, 0.05, 10, 0.08, 14, 0.11, 16, 0.14],
                'icon-allow-overlap': false,
                'icon-anchor': 'center'
            },
            paint: {
                'icon-halo-color': '#ffffff',
                'icon-halo-width': 2
            },
            minzoom: 12
        });

        // Addet power plant circles für Anlegn ohne Quelle
        map.addLayer({
            id: 'power-plants-circles',
            type: 'circle',
            source: 'oim-energy',
            'source-layer': 'power_plant_point',
            filter: [
                '!',
                ['any',
                    ['>=', ['index-of', 'coal', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'geothermal', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'hydro', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'water', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'nuclear', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'oil', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'gas', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'solar', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'photovoltaic', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'wind', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'biomass', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'waste', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0],
                    ['>=', ['index-of', 'battery', ['downcase', ['coalesce', ['get', 'plant:source'], ['get', 'generator:source'], ['get', 'source'], ['get', 'plant:method'], ['get', 'generator:method'], '']]], 0]
                ]
            ],
            paint: {
                'circle-color': '#6b7280',
                'circle-radius': ['interpolate', ['linear'], ['zoom'], 12, 4, 16, 6, 20, 8],
                'circle-opacity': 0.8,
                'circle-stroke-color': '#ffffff',
                'circle-stroke-width': 1
            },
            minzoom: 12
        });

        // ===== ENDE POWER PLANTS =====

        // Addet switches aus den OIM vector tiles
        map.addLayer({
            id: 'power-switches-oim',
            type: 'circle',
            source: 'oim-energy',
            'source-layer': 'power_switch',
            paint: {
                'circle-color': '#ef4444',
                'circle-radius': [
                    'interpolate', ['linear'], ['zoom'],
                    12, 2,
                    16, 3,
                    20, 5
                ],
                'circle-opacity': 0.8
            },
            layout: { visibility: 'none' }
        });






        // Addet control für alle OIM layers 
        const allOIMLayerIds = [
            'power-substations-fill', 
            'power-substations-outline',
            'power-lines-voltage',
            'power-cables-voltage-bg',
            'power-cables-voltage', 
            'power-minor-lines-voltage',
            'power-plants-icons',
            'power-plants-circles',
            'power-plants-oim',
            'power-generators-oim',
            'power-transformers-oim',
            'power-substation-points-oim',
            'power-switches-oim',
            // Addet alle  neuen Layer
            'oim-power_plant_point',
            'oim-power_generator',
            'oim-power_tower',
            'oim-power_transformer',
            'oim-power_substation_point',
            'oim-power_switch',
            'oim-power_compensator',
            'oim-power_converter',


        ];

        addOverlayControl('OpenInfraMap Energy', allOIMLayerIds, true);
        console.log('[OIM DEBUG] Added control for', allOIMLayerIds.length, 'layers');

        // click handlers für die OIM Layer
        const clickableLayers = [
            'power-plants-oim', 'power-generators-oim', 'power-substation-points-oim', 'power-transformers-oim',
            'power-plants-icons', 'power-plants-circles',  // New power plant layers
            'oim-power_generator', 'oim-power_tower', 'oim-power_transformer',
            'oim-power_substation_point', 'oim-power_switch', 'oim-power_compensator', 'oim-power_converter'
        ];

        clickableLayers.forEach(layerId => {
            map.on('click', layerId, function(e) {
                const feature = e.features[0];
                const props = feature.properties;

                let popupContent = `<div style="font-family: Arial, sans-serif;">`;
                popupContent += `<h3 style="margin: 0 0 10px 0; color: #333;">Power Infrastructure</h3>`;
                popupContent += `<p><strong>Layer:</strong> ${layerId}</p>`;

                // Zeit alle bekannten Eigenschaften
                Object.keys(props).forEach(key => {
                    if (props[key] && key !== 'layer') {
                        popupContent += `<p><strong>${key}:</strong> ${props[key]}</p>`;
                    }
                });

                popupContent += `<p style="font-size: 12px; color: #666; margin-top: 10px;">`;
                popupContent += `OpenInfraMap</p>`;
                popupContent += `</div>`;

                new maplibregl.Popup()
                    .setLngLat(e.lngLat)
                    .setHTML(popupContent)
                    .addTo(map);
            });

            // Verändern maus cursor beim hovern
            map.on('mouseenter', layerId, function() {
                map.getCanvas().style.cursor = 'pointer';
            });

            map.on('mouseleave', layerId, function() {
                map.getCanvas().style.cursor = '';
            });
        });


        map.on('click', 'power-lines-voltage', function(e) {
            const feature = e.features[0];
            const props = feature.properties;

            console.log('[DEBUG] Power line clicked - All properties:', props);
            console.log('[DEBUG] Voltage value:', props.voltage);
            console.log('[DEBUG] Available keys:', Object.keys(props));


            let debugContent = '<div><h3>DEBUG: Power Line Properties</h3>';
            Object.keys(props).forEach(key => {
                debugContent += `<p><strong>${key}:</strong> ${props[key]}</p>`;
            });
            debugContent += '</div>';

            new maplibregl.Popup()
                .setLngLat(e.lngLat)
                .setHTML(debugContent)
                .addTo(map);
        });

        // Funnktion die alle energy layer zeigt, wenn layer aktiviert
        window.toggleAllOIMLayers = function(visible) {
            const allLayers = map.getStyle().layers;
            const oimLayers = allLayers.filter(layer => 
                layer.id.startsWith('power-') || layer.id.startsWith('oim-')
            );

            oimLayers.forEach(layer => {
                map.setLayoutProperty(layer.id, 'visibility', visible ? 'visible' : 'none');
            });

            console.log('[OIM DEBUG] Toggled', oimLayers.length, 'layers to', visible ? 'visible' : 'hidden');
        };


        // nur benötigte Layer behalten
        try {
            const keep = new Set([
                'power-substations-fill',
                'power-substations-outline',
                'power-lines-voltage',
                'power-cables-voltage-bg',
                'power-cables-voltage',
                'power-minor-lines-voltage',
                'power-plants-icons',
                'power-plants-circles',
                'oim-power_tower'
            ]);
            const style = map.getStyle();
            if (style && Array.isArray(style.layers)) {
                style.layers
                  .filter(l => (l.id && (l.id.startsWith('oim-') || l.id.startsWith('power-') || l.id.startsWith('osm-'))) && !keep.has(l.id))
                  .forEach(l => { try { map.removeLayer(l.id); } catch(_) {} });
            }
        } catch(_) {}

        try {
            ['oim-power_plant_point','oim-power_tower'].forEach(id => {
                if (map.getLayer(id)) { map.moveLayer(id); }
            });
        } catch(_) {}
console.log('[ENERGY] ✓ Energy layer added with automatic layer discovery');

    } catch (error) {
        console.error('[ENERGY] Failed to add energy layer:', error);
    }
}

// Addet District Heating Networks layer
async function addDistrictHeatingLayer() {
    console.log('[DISTRICT_HEATING] Adding District Heating Networks layer...');

    try {
        const dhResponse = await fetch('/district_heating.geojson');
        const dhData = await dhResponse.json();

        map.addSource('district-heating', {
            type: 'geojson',
            data: dhData
        });

        map.addLayer({
            id: 'district-heating-fill',
            type: 'fill',
            source: 'district-heating',
            filter: ['==', ['geometry-type'], 'Polygon'],
            paint: {
                'fill-color': '#ffffff',
                'fill-opacity': 0.3
            },
            layout: { 'visibility': 'visible' }
        });

        map.addLayer({
            id: 'district-heating-line',
            type: 'line',
            source: 'district-heating',
            filter: ['any', ['==', ['geometry-type'], 'LineString'], ['==', ['geometry-type'], 'Polygon']],
            paint: {
                'line-color': '#ffffff',
                'line-width': 2,
                'line-opacity': 0.8
            },
            layout: { 'visibility': 'visible' }
        });

        console.log('[DISTRICT_HEATING] ✓ District Heating Networks added (20 km radius)');
        addOverlayControl('District Heating Networks', ['district-heating-fill', 'district-heating-line'], true, 'districtHeating');
    } catch (e) {
        console.warn('[DISTRICT_HEATING] District Heating Networks not available');
    }
}

// Helper function für einklappbare Gruppen
function createSimpleGroup(groupName, parentElement) {
    // Group container
    var groupDiv = document.createElement('div');
    groupDiv.style.marginTop = '8px';
    groupDiv.style.border = '1px solid #ddd';
    groupDiv.style.borderRadius = '4px';
    groupDiv.style.background = '#f9f9f9';

    // Group header 
    var headerDiv = document.createElement('div');
    headerDiv.style.padding = '6px 8px';
    headerDiv.style.background = '#f0f0f0';
    headerDiv.style.cursor = 'pointer';
    headerDiv.style.borderBottom = '1px solid #ddd';
    headerDiv.style.userSelect = 'none';

    var toggleSpan = document.createElement('span');
    toggleSpan.textContent = '▼ ';
    toggleSpan.style.fontSize = '10px';

    var nameSpan = document.createElement('span');
    nameSpan.textContent = groupName;
    nameSpan.style.fontWeight = 'bold';
    nameSpan.style.fontSize = '11px';

    headerDiv.appendChild(toggleSpan);
    headerDiv.appendChild(nameSpan);

    // Group content (
    var contentDiv = document.createElement('div');
    contentDiv.style.padding = '4px 8px';
    contentDiv.style.background = 'white';
    contentDiv.style.display = 'block'; // Start expanded

    // Toggle Funktion
    var collapsed = false;
    headerDiv.onclick = function() {
        collapsed = !collapsed;
        contentDiv.style.display = collapsed ? 'none' : 'block';
        toggleSpan.textContent = collapsed ? '▶ ' : '▼ ';
    };

    groupDiv.appendChild(headerDiv);
    groupDiv.appendChild(contentDiv);
    parentElement.appendChild(groupDiv);

    return {
        element: groupDiv,
        content: contentDiv,
        addLayer: function(layerElement) {
            contentDiv.appendChild(layerElement);
        }
    };
}

// Helper function 
function addOverlayControl(name, layerIds, checked = false, groupName = null) {

    var targetGroup = null;

    if (groupName && window.layerGroups && window.layerGroups[groupName]) {
        targetGroup = window.layerGroups[groupName];
    } else {

        if (name.toLowerCase().includes('wwtp') || name.toLowerCase().includes('outlines') || name.toLowerCase().includes('plant') || name.toLowerCase().includes('available area')) {
            targetGroup = window.layerGroups ? window.layerGroups['wwtp'] : null;
        } else if (name.toLowerCase().includes('calculated') || name.toLowerCase().includes('pipeline to')) {
            targetGroup = window.layerGroups ? window.layerGroups['calculatedPipelines'] : null;
        } else if (name.toLowerCase().includes('gas') || name.toLowerCase().includes('ehb') || name.toLowerCase().includes('pipeline')) {
            targetGroup = window.layerGroups ? window.layerGroups['gasTransmission'] : null;
        } else if (name.toLowerCase().includes('district heating')) {
            targetGroup = window.layerGroups ? window.layerGroups['districtHeating'] : null;
        } else if (name.toLowerCase().includes('energy') || name.toLowerCase().includes('power') || name.toLowerCase().includes('openinfra')) {
            targetGroup = window.layerGroups ? window.layerGroups['energy'] : null;
        } else if (name.toLowerCase().includes('protected') || name.toLowerCase().includes('natura')) {
            targetGroup = window.layerGroups ? window.layerGroups['protectedAreas'] : null;
        }
    }


    var label = document.createElement('label');
    label.style.display = 'block';
    label.style.cursor = 'pointer';
    label.style.margin = '4px 0';
    label.style.fontSize = '11px';

    var checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.checked = checked;
    checkbox.style.marginRight = '6px';
    checkbox.onchange = function() {
        var visibility = checkbox.checked ? 'visible' : 'none';


        var calculatedPipelineNames = ['Pipeline to EHB', 'Pipeline to Gaspipelines'];
        if (calculatedPipelineNames.includes(name)) {
            handleCalculatedPipelineToggle(name, checkbox.checked, layerIds);
        } else {

            layerIds.forEach(function(layerId) {
                try {
                    map.setLayoutProperty(layerId, 'visibility', visibility);
                } catch (error) {
                    console.warn('Failed to toggle layer ' + layerId + ':', error);
                }
            });
        }
    };

    label.appendChild(checkbox);


    try {
        var initialVisibility = checked ? 'visible' : 'none';
        layerIds.forEach(function(layerId) {
            try { map.setLayoutProperty(layerId, 'visibility', initialVisibility); } catch (err) {}
        });
    } catch (e) {}

    label.appendChild(document.createTextNode(' ' + name));


    if (targetGroup && targetGroup.addLayer) {
        targetGroup.addLayer(label);
        console.log('[CONTROL] Added "' + name + '" to group');
    } else {

        var overlaysSection = document.querySelector('#overlays-section') || document.querySelector('div:contains("Overlays")');
        if (overlaysSection) {
            overlaysSection.appendChild(label);
            console.log('[CONTROL] Added "' + name + '" to overlays section (no group)');
        } else {
            console.warn('[CONTROL] Could not find target for layer "' + name + '"');
        }
    }
}


function addSimpleLayerControl() {
    console.log('[CONTROL] Adding simple layer control...');

    try {
        var controlDiv = document.createElement('div');
        controlDiv.className = 'maplibregl-ctrl maplibregl-ctrl-group';
        controlDiv.style.background = 'white';
        controlDiv.style.padding = '10px';
        controlDiv.style.minWidth = '250px';


        // Titel
        var title = document.createElement('div');
        title.innerHTML = '<strong>Layers</strong>';
        title.style.marginBottom = '10px';
        controlDiv.appendChild(title);

        // Grud layer
        var baseSection = document.createElement('div');
        baseSection.innerHTML = '<strong>Base Maps</strong><br>';

        // Esri World Imagery
        var esriLabel = document.createElement('label');
        esriLabel.style.display = 'block';
        esriLabel.style.margin = '5px 0';
        var esriRadio = document.createElement('input');
        esriRadio.type = 'radio';
        esriRadio.name = 'basemap';
        esriRadio.checked = true;
        esriRadio.onchange = function() {
            if (this.checked) {
                map.setLayoutProperty('esri-imagery-layer', 'visibility', 'visible');
                map.setLayoutProperty('osm-layer', 'visibility', 'none');
            }
        };
        esriLabel.appendChild(esriRadio);
        esriLabel.appendChild(document.createTextNode(' Esri World Imagery'));
        baseSection.appendChild(esriLabel);

        // OSM
        var osmLabel = document.createElement('label');
        osmLabel.style.display = 'block';
        osmLabel.style.margin = '5px 0';
        var osmRadio = document.createElement('input');
        osmRadio.type = 'radio';
        osmRadio.name = 'basemap';
        osmRadio.onchange = function() {
            if (this.checked) {
                map.setLayoutProperty('esri-imagery-layer', 'visibility', 'none');
                map.setLayoutProperty('osm-layer', 'visibility', 'visible');
            }
        };
        osmLabel.appendChild(osmRadio);
        osmLabel.appendChild(document.createTextNode(' OSM Standard'));
        baseSection.appendChild(osmLabel);

        controlDiv.appendChild(baseSection);

        // Separator
        var hr = document.createElement('hr');
        controlDiv.appendChild(hr);

        // Overlays with simple groups
        var overlaySection = document.createElement('div');
        overlaySection.innerHTML = '<strong>Overlays</strong><br>';

        // Esri Reference (bleibt oben)
        var refLabel = document.createElement('label');
        refLabel.style.display = 'block';
        refLabel.style.margin = '5px 0';
        var refCheck = document.createElement('input');
        refCheck.type = 'checkbox';
        refCheck.checked = true;
        refCheck.onchange = function() {
            map.setLayoutProperty('esri-reference-layer', 'visibility', this.checked ? 'visible' : 'none');
        };
        refLabel.appendChild(refCheck);
        refLabel.appendChild(document.createTextNode(' Esri Reference'));
        overlaySection.appendChild(refLabel);

        // Erstellt group containers
        window.layerGroups = {
            'wwtp': createSimpleGroup('WWTP', overlaySection),
            'gasTransmission': createSimpleGroup('Gas Transmission Networks', overlaySection),
            'calculatedPipelines': createSimpleGroup('Calculated Pipelines', overlaySection),
            'energy': createSimpleGroup('Energy Networks', overlaySection),
            'districtHeating': createSimpleGroup('District Heating Networks', overlaySection),
            'protectedAreas': createSimpleGroup('Protected Areas', overlaySection)
        };

        controlDiv.appendChild(overlaySection);

        // Fügt gesamte control zu map hinzu
        map.addControl({
            onAdd: function() {
                return controlDiv;
            },
            onRemove: function() {}
        }, 'top-left');


        var legendButton = document.createElement('button');
        legendButton.className = 'maplibregl-ctrl-icon';
        legendButton.type = 'button';
        legendButton.title = 'Legend';
        legendButton.style.width = '29px';
        legendButton.style.height = '29px';
        legendButton.style.background = 'white';
        legendButton.style.border = 'none';
        legendButton.style.cursor = 'pointer';
        legendButton.style.display = 'flex';
        legendButton.style.alignItems = 'center';
        legendButton.style.justifyContent = 'center';
        legendButton.style.fontSize = '16px';
        legendButton.innerHTML = '🗺️'; // Map/Legend icon

        var legendPanel = document.createElement('div');
        legendPanel.style.display = 'none';
        legendPanel.style.position = 'absolute';
        legendPanel.style.top = '0';
        legendPanel.style.left = '40px'; // Position right of the button
        legendPanel.style.background = 'white';
        legendPanel.style.padding = '10px';
        legendPanel.style.borderRadius = '4px';
        legendPanel.style.boxShadow = '0 0 0 2px rgba(0,0,0,.1)';
        legendPanel.style.maxHeight = '80vh';
        legendPanel.style.overflowY = 'auto';
        legendPanel.style.minWidth = '400px';
        legendPanel.style.fontSize = '11px';
        legendPanel.style.lineHeight = '1.4';
        legendPanel.style.zIndex = '1';

        // Legenden Titel
        var legendTitle = document.createElement('div');
        legendTitle.innerHTML = '<strong>Legend</strong>';
        legendTitle.style.marginBottom = '10px';
        legendTitle.style.fontSize = '13px';
        legendPanel.appendChild(legendTitle);

        // zwei spalten
        var legendContent = document.createElement('div');
        legendContent.style.display = 'grid';
        legendContent.style.gridTemplateColumns = '1fr 1fr';
        legendContent.style.gap = '20px';
        legendContent.style.position = 'relative';


        var leftColumnHTML = '';

        // WWTP 
        leftColumnHTML += '<div style="margin-top: 8px;"><strong>WWTP</strong></div>';
        leftColumnHTML += '<div style="margin-left: 8px;">';
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="16" height="16"><rect x="3" y="3" width="10" height="10" fill="rgba(29, 78, 216, 0.1)" stroke="#1d4ed8" stroke-width="2"/></svg><span style="margin-left: 5px;">WWTP Outlines</span></div>';
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="16" height="16"><rect x="3" y="3" width="10" height="10" fill="rgba(34, 197, 94, 0.15)" stroke="#0f2419" stroke-width="2"/></svg><span style="margin-left: 5px;">Available Area</span></div>';
        leftColumnHTML += '</div>';

        // Gas Transmission Networks
        leftColumnHTML += '<div style="margin-top: 8px;"><strong>Gas Transmission Networks</strong></div>';
        leftColumnHTML += '<div style="margin-left: 8px;">';
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="5"><line x1="0" y1="2.5" x2="20" y2="2.5" stroke="#0B3A8C" stroke-width="3"/></svg><span style="margin-left: 5px;">EHB Transmission</span></div>';
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="5"><line x1="0" y1="2.5" x2="20" y2="2.5" stroke="#0ea5e9" stroke-width="3"/></svg><span style="margin-left: 5px;">EHB High Pressure</span></div>';
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="5"><line x1="0" y1="2.5" x2="20" y2="2.5" stroke="#f59e0b" stroke-width="2"/></svg><span style="margin-left: 5px;">Gas Pipelines</span></div>';
        leftColumnHTML += '</div>';

        // Calculated Pipelines
        leftColumnHTML += '<div style="margin-top: 8px;"><strong>Calculated Pipelines</strong></div>';
        leftColumnHTML += '<div style="margin-left: 8px;">';
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="8"><line x1="0" y1="4" x2="20" y2="4" stroke="#ffffff" stroke-width="5"/><line x1="0" y1="4" x2="20" y2="4" stroke="#1a1a1a" stroke-width="3"/></svg><span style="margin-left: 5px;">Calculated Routes</span></div>';
        leftColumnHTML += '<div style="margin-left: 12px; font-size: 10px; color: #666;">';
        leftColumnHTML += '<div>• Pipeline to EHB</div>';
        leftColumnHTML += '<div>• Pipeline to Gaspipelines</div>';
        leftColumnHTML += '</div>';
        leftColumnHTML += '</div>';

        // District Heating Networks
        leftColumnHTML += '<div style="margin-top: 8px;"><strong>District Heating Networks</strong></div>';
        leftColumnHTML += '<div style="margin-left: 8px;">';
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="16" height="16"><rect x="3" y="3" width="10" height="10" fill="rgba(255, 255, 255, 0.3)" stroke="#ffffff" stroke-width="2"/></svg><span style="margin-left: 5px;">District Heating Areas</span></div>';
        leftColumnHTML += '</div>';

        // Protected Areas
        leftColumnHTML += '<div style="margin-top: 8px;"><strong>Protected Areas</strong></div>';
        leftColumnHTML += '<div style="margin-left: 8px;">';
        leftColumnHTML += '<div style="font-size: 10px; color: #666; font-style: italic; margin-bottom: 4px;">Colors from EEA WMS Service</div>';

        // Natura 2000
        leftColumnHTML += '<div style="margin-top: 4px;"><em>Natura 2000 Sites</em></div>';
        leftColumnHTML += '<div style="margin-left: 8px; font-size: 10px;">';

        // Habitats Directive - Blue diagonal lines (135° - top-left to bottom-right)
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;">';
        leftColumnHTML += '<svg width="14" height="14"><defs><pattern id="hatch-blue" x="0" y="0" width="4" height="4" patternUnits="userSpaceOnUse" patternTransform="rotate(-45)"><line x1="0" y1="0" x2="0" y2="4" stroke="#0000ff" stroke-width="1"/></pattern></defs><rect width="14" height="14" fill="url(#hatch-blue)" stroke="#0000ff" stroke-width="1"/></svg>';
        leftColumnHTML += '<span style="margin-left: 5px;">Habitats Directive Sites (pSCI, SCI or SAC)</span></div>';

        // Birds Directive - Red diagonal lines (45° - top-right to bottom-left)
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;">';
        leftColumnHTML += '<svg width="14" height="14"><defs><pattern id="hatch-red" x="0" y="0" width="4" height="4" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><line x1="0" y1="0" x2="0" y2="4" stroke="#ff0000" stroke-width="1"/></pattern></defs><rect width="14" height="14" fill="url(#hatch-red)" stroke="#ff0000" stroke-width="1"/></svg>';
        leftColumnHTML += '<span style="margin-left: 5px;">Birds Directive Sites (SPA)</span></div>';

        leftColumnHTML += '</div>';

        // NatDB Protected Areas
        leftColumnHTML += '<div style="margin-top: 6px;"><em>Nationally Designated Areas (NatDA)</em></div>';
        leftColumnHTML += '<div style="margin-left: 8px; font-size: 10px;">';
        leftColumnHTML += '<div style="margin-bottom: 2px; font-weight: bold;">IUCN Categories:</div>';

        // Ia -
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;">';
        leftColumnHTML += '<svg width="14" height="14"><defs><pattern id="dots-ia" x="0" y="0" width="4" height="4" patternUnits="userSpaceOnUse"><circle cx="2" cy="2" r="0.8" fill="#228b22"/></pattern></defs><rect width="14" height="14" fill="url(#dots-ia)" stroke="#228b22" stroke-width="1"/></svg>';
        leftColumnHTML += '<span style="margin-left: 5px;">Ia - Strict Nature Reserve</span></div>';

        // Ib - 
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;">';
        leftColumnHTML += '<svg width="14" height="14"><defs><pattern id="dots-ib" x="0" y="0" width="4" height="4" patternUnits="userSpaceOnUse"><circle cx="2" cy="2" r="0.8" fill="#cccc00"/></pattern></defs><rect width="14" height="14" fill="url(#dots-ib)" stroke="#cccc00" stroke-width="1"/></svg>';
        leftColumnHTML += '<span style="margin-left: 5px;">Ib - Wilderness Area</span></div>';

        // II - 
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;">';
        leftColumnHTML += '<svg width="14" height="14"><defs><pattern id="dots-ii" x="0" y="0" width="4" height="4" patternUnits="userSpaceOnUse"><circle cx="2" cy="2" r="0.8" fill="#006400"/></pattern></defs><rect width="14" height="14" fill="url(#dots-ii)" stroke="#006400" stroke-width="1"/></svg>';
        leftColumnHTML += '<span style="margin-left: 5px;">II - National Park</span></div>';

        // III - 
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;">';
        leftColumnHTML += '<svg width="14" height="14"><defs><pattern id="dots-iii" x="0" y="0" width="4" height="4" patternUnits="userSpaceOnUse"><circle cx="2" cy="2" r="0.8" fill="#cccc00"/></pattern></defs><rect width="14" height="14" fill="url(#dots-iii)" stroke="#cccc00" stroke-width="1"/></svg>';
        leftColumnHTML += '<span style="margin-left: 5px;">III - Natural Monument or Feature</span></div>';

        // IV - 
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;">';
        leftColumnHTML += '<svg width="14" height="14"><defs><pattern id="dots-iv" x="0" y="0" width="4" height="4" patternUnits="userSpaceOnUse"><circle cx="2" cy="2" r="0.8" fill="#ff8c00"/></pattern></defs><rect width="14" height="14" fill="url(#dots-iv)" stroke="#ff8c00" stroke-width="1"/></svg>';
        leftColumnHTML += '<span style="margin-left: 5px;">IV - Habitat/Species Management Area</span></div>';

        // V - 
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;">';
        leftColumnHTML += '<svg width="14" height="14"><defs><pattern id="dots-v" x="0" y="0" width="4" height="4" patternUnits="userSpaceOnUse"><circle cx="2" cy="2" r="0.8" fill="#cc00cc"/></pattern></defs><rect width="14" height="14" fill="url(#dots-v)" stroke="#cc00cc" stroke-width="1"/></svg>';
        leftColumnHTML += '<span style="margin-left: 5px;">V - Protected Landscape/Seascape</span></div>';

        // VI - 
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;">';
        leftColumnHTML += '<svg width="14" height="14"><defs><pattern id="dots-vi" x="0" y="0" width="4" height="4" patternUnits="userSpaceOnUse"><circle cx="2" cy="2" r="0.8" fill="#0000cc"/></pattern></defs><rect width="14" height="14" fill="url(#dots-vi)" stroke="#0000cc" stroke-width="1"/></svg>';
        leftColumnHTML += '<span style="margin-left: 5px;">VI - Sustainable Use of Natural Resources</span></div>';

        // Other - 
        leftColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;">';
        leftColumnHTML += '<svg width="14" height="14"><defs><pattern id="dots-other" x="0" y="0" width="4" height="4" patternUnits="userSpaceOnUse"><circle cx="2" cy="2" r="0.8" fill="#a9a9a9"/></pattern></defs><rect width="14" height="14" fill="url(#dots-other)" stroke="#a9a9a9" stroke-width="1"/></svg>';
        leftColumnHTML += '<span style="margin-left: 5px;">Other (UA, NA, not applicable/reported/assigned)</span></div>';

        leftColumnHTML += '</div>';

        leftColumnHTML += '</div>';

        // Rechte Spalte - Energy Networks
        var rightColumnHTML = '';
        rightColumnHTML += '<div style="margin-top: 8px;"><strong>Energy Networks</strong></div>';
        rightColumnHTML += '<div style="margin-left: 8px;">';
        rightColumnHTML += '<div style="margin-top: 4px;"><em>Power Lines (Voltage)</em></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="3"><line x1="0" y1="1.5" x2="20" y2="1.5" stroke="#00cccc" stroke-width="2"/></svg><span style="margin-left: 5px;">≥ 550 kV</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="3"><line x1="0" y1="1.5" x2="20" y2="1.5" stroke="#cc00cc" stroke-width="2"/></svg><span style="margin-left: 5px;">≥ 310 kV</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="3"><line x1="0" y1="1.5" x2="20" y2="1.5" stroke="#cc0000" stroke-width="2"/></svg><span style="margin-left: 5px;">≥ 220 kV</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="3"><line x1="0" y1="1.5" x2="20" y2="1.5" stroke="#cc6600" stroke-width="2"/></svg><span style="margin-left: 5px;">≥ 132 kV</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="3"><line x1="0" y1="1.5" x2="20" y2="1.5" stroke="#cccc00" stroke-width="2"/></svg><span style="margin-left: 5px;">≥ 52 kV</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="3"><line x1="0" y1="1.5" x2="20" y2="1.5" stroke="#00cc00" stroke-width="2"/></svg><span style="margin-left: 5px;">≥ 25 kV</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="3"><line x1="0" y1="1.5" x2="20" y2="1.5" stroke="#6699ff" stroke-width="2"/></svg><span style="margin-left: 5px;">≥ 10 kV</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="3"><line x1="0" y1="1.5" x2="20" y2="1.5" stroke="#808080" stroke-width="2"/></svg><span style="margin-left: 5px;">&lt; 10 kV</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="3"><line x1="0" y1="1.5" x2="20" y2="1.5" stroke="#800080" stroke-width="2"/></svg><span style="margin-left: 5px;">DC (HVDC)</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="20" height="3"><line x1="0" y1="1.5" x2="20" y2="1.5" stroke="#a0a0a0" stroke-width="2"/></svg><span style="margin-left: 5px;">Traction (&lt;50 Hz)</span></div>';
        rightColumnHTML += '<div style="margin-top: 4px;"><em>Power Cables (without Tower, Voltage colors)</em></div>';

        // Power Plants 
        rightColumnHTML += '<div style="margin-top: 4px;"><em>Power Plants by Type</em></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><img src="/icons/plant-coal.png" width="20" height="20" style="image-rendering: crisp-edges;"/><span style="margin-left: 5px;">Coal</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><img src="/icons/plant-nuclear.png" width="20" height="20" style="image-rendering: crisp-edges;"/><span style="margin-left: 5px;">Nuclear</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><img src="/icons/plant-oilgas.png" width="20" height="20" style="image-rendering: crisp-edges;"/><span style="margin-left: 5px;">Oil/Gas</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><img src="/icons/plant-hydro.png" width="20" height="20" style="image-rendering: crisp-edges;"/><span style="margin-left: 5px;">Hydro</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><img src="/icons/plant-solar.png" width="20" height="20" style="image-rendering: crisp-edges;"/><span style="margin-left: 5px;">Solar</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><img src="/icons/plant-wind.png" width="20" height="20" style="image-rendering: crisp-edges;"/><span style="margin-left: 5px;">Wind</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><img src="/icons/plant-biomass.png" width="20" height="20" style="image-rendering: crisp-edges;"/><span style="margin-left: 5px;">Biomass</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><img src="/icons/plant-geothermal.png" width="20" height="20" style="image-rendering: crisp-edges;"/><span style="margin-left: 5px;">Geothermal</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><img src="/icons/plant-waste.png" width="20" height="20" style="image-rendering: crisp-edges;"/><span style="margin-left: 5px;">Waste</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><img src="/icons/plant-battery.png" width="20" height="20" style="image-rendering: crisp-edges;"/><span style="margin-left: 5px;">Battery Storage</span></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="16" height="16"><circle cx="8" cy="8" r="4" fill="#6b7280" stroke="#fff" stroke-width="1"/></svg><span style="margin-left: 5px;">Other/Unknown</span></div>';

        // Substations & Infrastructure
        rightColumnHTML += '<div style="margin-top: 4px;"><em>Substations & Infrastructure</em></div>';
        rightColumnHTML += '<div style="display: flex; align-items: center; margin: 2px 0;"><svg width="16" height="16"><rect x="3" y="3" width="10" height="10" fill="rgba(59, 130, 246, 0.2)" stroke="#1d4ed8" stroke-width="1.5"/></svg><span style="margin-left: 5px;">Substations</span></div>';
        rightColumnHTML += '</div>';

        // Spalten gliedern
        var leftColumn = document.createElement('div');
        leftColumn.innerHTML = leftColumnHTML;
        leftColumn.style.paddingRight = '10px';

        var rightColumn = document.createElement('div');
        rightColumn.innerHTML = rightColumnHTML;
        rightColumn.style.paddingLeft = '10px';
        rightColumn.style.borderLeft = '1px solid #ddd';

        legendContent.appendChild(leftColumn);
        legendContent.appendChild(rightColumn);
        legendPanel.appendChild(legendContent);

        // Toggle legend panel
        var legendVisible = false;
        legendButton.onclick = function() {
            legendVisible = !legendVisible;
            legendPanel.style.display = legendVisible ? 'block' : 'none';
            legendButton.style.background = legendVisible ? '#e0e0e0' : 'white';
        };

        // Erstellt legenden controll container
        var legendControlDiv = document.createElement('div');
        legendControlDiv.className = 'maplibregl-ctrl maplibregl-ctrl-group';
        legendControlDiv.style.position = 'absolute';
        legendControlDiv.style.top = '0px';
        legendControlDiv.style.left = '280px'; // Position right of layer control
        legendControlDiv.style.zIndex = '1';
        legendControlDiv.appendChild(legendButton);
        legendControlDiv.appendChild(legendPanel);


        document.querySelector('.maplibregl-ctrl-top-left').appendChild(legendControlDiv);

        console.log('[CONTROL] ✓ Simple layer control added');
        console.log('[LEGEND] ✓ Legend control added');

    } catch (error) {
        console.error('[CONTROL] Failed to add layer control:', error);
    }
}


function addAllOverlayLayers() {
    console.log('[OVERLAYS] Adding overlay layers...');

    // Try to load WWTP outlines
    fetch('/outlines.geojson')
        .then(response => response.json())
        .then(data => {
            map.addSource('wwtp-outlines', {
                type: 'geojson',
                data: data
            });

            map.addLayer({
                id: 'wwtp-outlines-fill',
                type: 'fill',
                source: 'wwtp-outlines',
                paint: {
                    'fill-color': '#1d4ed8',
                    'fill-opacity': 0.1
                },
                layout: { 'visibility': 'visible' }
            });

            map.addLayer({
                id: 'wwtp-outlines-line',
                type: 'line',
                source: 'wwtp-outlines',
                paint: {
                    'line-color': '#1d4ed8',
                    'line-width': 2,
                    'line-opacity': 0.8
                },
                layout: { 'visibility': 'visible' }
            });

            console.log('[OVERLAYS] ✓ WWTP Outlines added');
            addOverlayControl('WWTP Outlines', ['wwtp-outlines-fill', 'wwtp-outlines-line'], true);
        })
        .catch(e => console.warn('[OVERLAYS] WWTP Outlines not available'));

    // versucht available area zu laden
    fetch('/freearea.geojson')
        .then(response => response.json())
        .then(data => {
            map.addSource('available-area', {
                type: 'geojson',
                data: data
            });

            map.addLayer({
                id: 'available-area-fill',
                type: 'fill',
                source: 'available-area',
                paint: {
                    'fill-color': '#22c55e',
                    'fill-opacity': 0.15
                },
                layout: { 'visibility': 'visible' }
            });

            // Addet style
            map.addLayer({
                id: 'available-area-outline',
                type: 'line',
                source: 'available-area',
                paint: {
                    'line-color': '#0f2419',  // Very dark green
                    'line-width': 2,
                    'line-opacity': 0.8
                },
                layout: { 'visibility': 'visible' }
            });

            // Addet FreeArea icon
            map.addLayer({
                id: 'available-area-icons',
                type: 'symbol',
                source: 'available-area',
                layout: {
                    'icon-image': 'free-area',
                    'icon-size': ['interpolate', ['linear'], ['zoom'], 10, 0.03, 14, 0.05, 18, 0.08],
                    'icon-anchor': 'center',
                    'icon-allow-overlap': false,
                    'visibility': 'visible'
                },
                paint: {
                    'icon-opacity': [
                        'interpolate', ['linear'], ['zoom'],
                        10, 0.6,   // Fade in at zoom 10
                        12, 0.8,   // More visible at zoom 12
                        16, 1.0    // Full opacity at high zoom
                    ]
                },
                minzoom: 10  // Only show at higher zoom levels
            });

            console.log('[OVERLAYS] ✓ Available Area added');
            addOverlayControl('Available Area', ['available-area-fill', 'available-area-outline', 'available-area-icons'], true);
        })
        .catch(e => console.warn('[OVERLAYS] Available Area not available'));

    // Addet Pipeline layers
    addPipelineLayers();

    // Addet Protected Areas
    addProtectedAreas();
}

// Funktion zum laden aller calculated pipelines 
async function loadCalculatedPipelinesWithPriority() {
    console.log('[CALC_PIPELINES] Loading calculated pipelines with priority logic...');

    var pipelineConfigs = [
        { file: '/routes_ehb_clip.geojson', name: 'Pipeline to EHB', color: '#0a0a0a', priority: 1 },
        { file: '/routes_gaspipelines_clip.geojson', name: 'Pipeline to Gaspipelines', color: '#1a1a1a', priority: 2 }
    ];

    var availablePipelines = [];
    var activePipeline = null;


    for (let config of pipelineConfigs) {
        try {
            const response = await fetch(config.file);
            if (response.ok) {
                const data = await response.json();
                availablePipelines.push({ ...config, data: data });
                console.log('[CALC_PIPELINES] ✓', config.name, 'available');


                if (!activePipeline || config.priority < activePipeline.priority) {
                    activePipeline = config;
                }
            }
        } catch (e) {
            console.warn('[CALC_PIPELINES]', config.name, 'not available');
        }
    }

    // Läd alle pipelines welche verfügbar sind
    availablePipelines.forEach(function(pipeline) {
        var sourceId = pipeline.name.toLowerCase().replace(/\\s+/g, '-');
        var isActive = activePipeline && pipeline.name === activePipeline.name;

        map.addSource(sourceId, {
            type: 'geojson',
            data: pipeline.data
        });

        // Style außen
        map.addLayer({
            id: sourceId + '-casing',
            type: 'line',
            source: sourceId,
            paint: {
                'line-color': '#ffffff',
                'line-width': 5,
                'line-opacity': 0.9
            },
            layout: { 'visibility': isActive ? 'visible' : 'none' }
        });

        // Style innen
        map.addLayer({
            id: sourceId + '-core',
            type: 'line',
            source: sourceId,
            paint: {
                'line-color': '#1a1a1a',
                'line-width': 3,
                'line-opacity': 1.0
            },
            layout: { 'visibility': isActive ? 'visible' : 'none' }
        });

        console.log('[CALC_PIPELINES] ✓', pipeline.name, 'loaded', isActive ? '(ACTIVE)' : '(inactive)');
        addOverlayControl(pipeline.name, [sourceId + '-casing', sourceId + '-core'], isActive);
    });

    if (activePipeline) {
        console.log('[CALC_PIPELINES] ✓ Active pipeline:', activePipeline.name, '(Priority', activePipeline.priority + ')');
    } else {
        console.log('[CALC_PIPELINES] ⚠ No calculated pipelines available');
    }

    // SPweichern
    window.activeCalculatedPipeline = activePipeline ? activePipeline.name : null;
}

// Funktion für Layerreihenfolge
async function loadPipelineLayersInOrder() {
    console.log('[PIPELINES] Loading layers in correct z-order...');

    try {

        try {
            const gasResponse = await fetch('/gasmains.geojson');
            const gasData = await gasResponse.json();

            map.addSource('gas-mains', {
                type: 'geojson',
                data: gasData
            });

            map.addLayer({
                id: 'gas-mains-line',
                type: 'line',
                source: 'gas-mains',
                paint: {
                    'line-color': '#f59e0b',
                    'line-width': 2,
                    'line-opacity': 0.9
                },
                layout: { 'visibility': 'visible' }
            });

            console.log('[PIPELINES] ✓ Gas Pipelines added (bottom layer)');
            addOverlayControl('Gas Pipelines', ['gas-mains-line'], true);
        } catch (e) {
            console.warn('[PIPELINES] Gas Pipelines not available');
        }


        try {
            const ehbHpResponse = await fetch('/ehb_hp.json');
            const ehbHpData = await ehbHpResponse.json();

            map.addSource('ehb-hp', {
                type: 'geojson',
                data: ehbHpData
            });

            map.addLayer({
                id: 'ehb-hp-line',
                type: 'line',
                source: 'ehb-hp',
                paint: {
                    'line-color': '#0ea5e9',
                    'line-width': 3,
                    'line-opacity': 0.9
                },
                layout: { 'visibility': 'visible' }
            });

            console.log('[PIPELINES] ✓ EHB High Pressure added (middle layer)');
            addOverlayControl('EHB High Pressure', ['ehb-hp-line'], true);
        } catch (e) {
            console.warn('[PIPELINES] EHB High Pressure not available');
        }


        try {
            const ehbTransResponse = await fetch('/ehb_transmission.json');
            const ehbTransData = await ehbTransResponse.json();

            map.addSource('ehb-transmission', {
                type: 'geojson',
                data: ehbTransData
            });

            map.addLayer({
                id: 'ehb-transmission-line',
                type: 'line',
                source: 'ehb-transmission',
                paint: {
                    'line-color': '#0B3A8C',
                    'line-width': 3,
                    'line-opacity': 0.9
                },
                layout: { 'visibility': 'visible' }
            });

            console.log('[PIPELINES] ✓ EHB Transmission added (top layer)');
            addOverlayControl('EHB Transmission', ['ehb-transmission-line'], true);
        } catch (e) {
            console.warn('[PIPELINES] EHB Transmission not available');
        }

        console.log('[PIPELINES] ✓ All pipeline layers loaded in correct z-order');

    } catch (error) {
        console.error('[PIPELINES] Failed to load pipeline layers:', error);
    }
}

// Addet pipeline layers
function addPipelineLayers() {
    console.log('[PIPELINES] Adding pipeline layers...');


    loadPipelineLayersInOrder();


    loadCalculatedPipelinesWithPriority();
}

// Addet Protected Areas von den WMS services der EEA
function addProtectedAreas() {
    console.log('[PROTECTED] Adding protected areas from EEA...');

    try {
        // Addet NatDB Protected Areas von den WMS raster quellen der EEA
        map.addSource('protected-natdb', {
            type: 'raster',
            tiles: [
                'https://bio.discomap.eea.europa.eu/arcgis/services/ProtectedSites/NatDAv22_Dyna_WM/MapServer/WMSServer?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image/png&TRANSPARENT=true&LAYERS=0&CRS=EPSG:3857&STYLES=&WIDTH=256&HEIGHT=256&BBOX={bbox-epsg-3857}'
            ],
            tileSize: 256
        });

        map.addLayer({
            id: 'protected-natdb-layer',
            type: 'raster',
            source: 'protected-natdb',
            paint: {
                'raster-opacity': 0.7
            },
            layout: { 'visibility': 'none' }
        });

        console.log('[PROTECTED] ✓ NatDB layer added');
        addOverlayControl('Protected Areas (NatDB)', ['protected-natdb-layer'], false);

        // Addet Natura 2000 Protected Areas als WMS raster quelle von der EEA
        map.addSource('protected-natura2000', {
            type: 'raster',
            tiles: [
                'https://bio.discomap.eea.europa.eu/arcgis/services/ProtectedSites/Natura2000_Dyna_WM/MapServer/WMSServer?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&FORMAT=image/png&TRANSPARENT=true&LAYERS=0&CRS=EPSG:3857&STYLES=&WIDTH=256&HEIGHT=256&BBOX={bbox-epsg-3857}'
            ],
            tileSize: 256
        });

        map.addLayer({
            id: 'protected-natura2000-layer',
            type: 'raster',
            source: 'protected-natura2000',
            paint: {
                'raster-opacity': 0.7
            },
            layout: { 'visibility': 'none' }
        });

        console.log('[PROTECTED] ✓ Natura 2000 layer added');
        addOverlayControl('Protected Areas (Natura 2000)', ['protected-natura2000-layer'], false);

        console.log('[PROTECTED] ✓ All protected areas loaded from EEA services');

    } catch (error) {
        console.error('[PROTECTED] Failed to load protected areas:', error);
        // Fallback to placeholders
        addOverlayControl('Protected Areas (NatDB)', [], false);
        addOverlayControl('Protected Areas (Natura 2000)', [], false);
    }
}

// Addet WWTP Icon mit anlagen namen label
function addWWTPIcon(lat, lon, plantName) {
    console.log('[WWTP] Adding WWTP icon at', lat, lon, 'with name:', plantName || 'Unknown Plant');

    try {
        // Check if source already exists
        if (map.getSource('wwtp-point')) {
            console.log('[WWTP] Removing existing WWTP layers');
            if (map.getLayer('wwtp-icon')) {
                map.removeLayer('wwtp-icon');
            }
            map.removeSource('wwtp-point');
        }


        map.addSource('wwtp-point', {
            type: 'geojson',
            data: {
                type: 'Feature',
                geometry: {
                    type: 'Point',
                    coordinates: [lon, lat]
                },
                properties: {
                    name: plantName || 'WWTP',
                    type: 'wwtp'
                }
            }
        });

        console.log('[WWTP] Source added, trying to load image...');


        const iconUrl = '/wwtp-icon.png';

        map.loadImage(iconUrl, function(error, image) {
            if (error) {
                console.error('[WWTP] Image load failed:', error);
                console.log('[WWTP] Adding visible red circle as fallback');


                map.addLayer({
                    id: 'wwtp-icon',
                    type: 'circle',
                    source: 'wwtp-point',
                    paint: {
                        'circle-radius': [
                            'interpolate', ['linear'], ['zoom'],
                            5, 2,      // Very small at zoom 5 (4x smaller)
                            8, 3,      // Still small at zoom 8
                            10, 4      // Small at zoom 10 (4x smaller)
                        ],
                        'circle-color': '#dc2626',
                        'circle-stroke-color': '#ffffff',
                        'circle-stroke-width': 1,  // Thinner stroke for smaller icon
                        'circle-opacity': [
                            'step', ['zoom'],
                            0.9,       // Default: visible
                            10, 0      // Hidden at zoom 10+
                        ]
                    }
                });



                // Bewegt wwtp ivon nach oben als Top-Layer
                function moveWWTPCircleToTop() {
                    try {
                        if (map.getLayer('wwtp-icon')) {
                            map.moveLayer('wwtp-icon');
                            console.log('[WWTP] Circle moved to top');
                        }
                    } catch (e) {
                        console.warn('[WWTP] Could not move layer to top:', e);
                    }
                }


                setTimeout(moveWWTPCircleToTop, 100);


                setInterval(moveWWTPCircleToTop, 2000);  // Every 2 seconds


                map.on('styledata', moveWWTPCircleToTop);

                console.log('[WWTP] ✓ Circle fallback added - visible when zoomed out (zoom < 11)');


                addWWTPTextOverlay(lat, lon, plantName || 'WWTP');

                return;
            }

            console.log('[WWTP] Image loaded successfully, adding symbol layer');


            if (map.hasImage('wwtp-icon-image')) {
                map.removeImage('wwtp-icon-image');
            }


            map.addImage('wwtp-icon-image', image);


            map.addLayer({
                id: 'wwtp-icon',
                type: 'symbol',
                source: 'wwtp-point',
                layout: {
                    'icon-image': 'wwtp-icon-image',
                    'icon-size': [
                        'interpolate', ['linear'], ['zoom'],
                        5, 0.1,    // Very small at zoom 5 (4x smaller than before)
                        8, 0.15,   // Still small at zoom 8
                        10, 0.2    // Small at zoom 10 (4x smaller than before)
                    ],
                    'icon-anchor': 'center',
                    'icon-allow-overlap': true,
                    'visibility': 'visible'
                },
                paint: {
                    'icon-opacity': [
                        'step', ['zoom'],
                        1.0,       // Default: visible
                        10, 0      // Hidden at zoom 10+
                    ]
                }
            });


            function moveWWTPToTop() {
                try {
                    if (map.getLayer('wwtp-icon')) {
                        map.moveLayer('wwtp-icon');
                        console.log('[WWTP] Icon moved to top layer');
                    }
                } catch (e) {
                    console.warn('[WWTP] Could not move layer to top:', e);
                }
            }


            setTimeout(moveWWTPToTop, 100);


            setInterval(moveWWTPToTop, 2000);  // Every 2 seconds


            map.on('styledata', moveWWTPToTop);

            console.log('[WWTP] ✓ Image icon added successfully - visible when zoomed out (zoom < 11)');


            addWWTPTextOverlay(lat, lon, plantName || 'WWTP');
        });

    } catch (error) {
        console.error('[WWTP] Critical error:', error);


        try {
            map.addSource('wwtp-emergency', {
                type: 'geojson',
                data: {
                    type: 'Feature',
                    geometry: {
                        type: 'Point',
                        coordinates: [lon, lat]
                    }
                }
            });

            map.addLayer({
                id: 'wwtp-emergency',
                type: 'circle',
                source: 'wwtp-emergency',
                paint: {
                    'circle-radius': 20,
                    'circle-color': '#ff0000',
                    'circle-opacity': 1.0
                }
            });

            console.log('[WWTP] Emergency red circle added');
        } catch (e) {
            console.error('[WWTP] Even emergency fallback failed:', e);
        }
    }
}


function addWWTPTextOverlay(lat, lon, plantName) {
    console.log('[WWTP] Adding HTML text overlay for:', plantName);

    try {

        const existingOverlay = document.getElementById('wwtp-text-overlay');
        if (existingOverlay) {
            existingOverlay.remove();
        }


        const textOverlay = document.createElement('div');
        textOverlay.id = 'wwtp-text-overlay';
        textOverlay.style.position = 'absolute';
        textOverlay.style.color = 'white';
        textOverlay.style.fontWeight = 'bold';
        textOverlay.style.fontSize = '12px';
        textOverlay.style.textShadow = '1px 1px 2px black, -1px -1px 2px black, 1px -1px 2px black, -1px 1px 2px black';
        textOverlay.style.pointerEvents = 'none';
        textOverlay.style.zIndex = '1000';
        textOverlay.style.whiteSpace = 'nowrap';
        textOverlay.style.transform = 'translate(-50%, -100%)';
        textOverlay.textContent = plantName;
        textOverlay.style.display = 'none'; // Initially hidden


        const mapContainer = document.getElementById('map');
        mapContainer.appendChild(textOverlay);


        function updateTextPosition() {
            const zoom = map.getZoom();


            if (zoom < 11) {
                const point = map.project([lon, lat]);
                textOverlay.style.left = point.x + 'px';
                textOverlay.style.top = (point.y - 15) + 'px'; // 15px above icon
                textOverlay.style.display = 'block';


                const fontSize = Math.max(8, Math.min(14, 8 + (zoom - 5) * 1.2));
                textOverlay.style.fontSize = fontSize + 'px';
            } else {
                textOverlay.style.display = 'none';
            }
        }


        map.on('move', updateTextPosition);
        map.on('zoom', updateTextPosition);
        map.on('resize', updateTextPosition);


        setTimeout(updateTextPosition, 100);

        console.log('[WWTP] ✓ HTML text overlay added');

    } catch (error) {
        console.error('[WWTP] Failed to add text overlay:', error);
    }
}




function getGroupForLayer(layerName) {
    const groupMappings = {
        'WWTP Outlines': 'WWTP',
        'Available Area': 'WWTP',
        'EHB Transmission': 'Gas Transmission Networks',
        'EHB High Pressure': 'Gas Transmission Networks', 
        'Gas Pipelines': 'Gas Transmission Networks',
        'Pipeline to EHB': 'Calculated Pipelines',
        'Pipeline to Gaspipelines': 'Calculated Pipelines',
        'OpenInfraMap Energy': 'Energy',
        'Protected Areas (NatDB)': 'Protected Areas',
        'Protected Areas (Natura 2000)': 'Protected Areas'
    };

    return groupMappings[layerName] || null;
}


function getZIndexForLayer(layerName) {
    const zIndexMap = {
        'EHB Transmission': 1100,
        'EHB High Pressure': 1050,
        'Gas Pipelines': 950,
        'Pipeline to EHB': 850,
        'Pipeline to Gaspipelines': 825,
        'WWTP Outlines': 750,
        'Available Area': 725,
        'OpenInfraMap Energy': 650,
        'Protected Areas (NatDB)': 550,
        'Protected Areas (Natura 2000)': 525
    };

    return zIndexMap[layerName] || 500;
}


function addLegacyOverlayControl(name, layerIds, checked) {
    try {

        var groupKey = getGroupKeyForLayer(name);
        var targetGroup = window.layerGroups && window.layerGroups[groupKey];


        var label = document.createElement('label');
        label.style.display = 'block';
        label.style.margin = '4px 0';
        label.style.fontSize = '11px';

        var checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = checked;
        checkbox.onchange = function() {

            if (name.startsWith('Pipeline to ')) {
                handleCalculatedPipelineToggle(name, this.checked, layerIds);
            } else {
                // Regular layer toggle
                var visibility = this.checked ? 'visible' : 'none';
                layerIds.forEach(function(layerId) {
                    try {
                        map.setLayoutProperty(layerId, 'visibility', visibility);
                    } catch(e) {
                        console.warn('[CONTROL] Layer not found:', layerId);
                    }
                });
                console.log('[CONTROL]', name, ':', this.checked ? 'ON' : 'OFF');
            }
        };

        label.appendChild(checkbox);
        label.appendChild(document.createTextNode(' ' + name));


        if (targetGroup) {
            targetGroup.addLayer(label);
            console.log('[CONTROL]', name, 'added to group:', groupKey);
        } else {

            var controlDiv = document.querySelector('.maplibregl-ctrl-group');
            if (controlDiv) {
                controlDiv.appendChild(label);
            }
            console.warn('[CONTROL] No group found for', name, ', added to main area');
        }

    } catch (error) {
        console.error('[CONTROL] Failed to add overlay control for', name, ':', error);
    }
}


function handleCalculatedPipelineToggle(layerName, isChecked, layerIds) {
    console.log('[MUTUAL_EXCLUSIVE] Handling', layerName, ':', isChecked ? 'ON' : 'OFF');

    var calculatedPipelineNames = [
        'Pipeline to EHB',
        'Pipeline to Gaspipelines'
    ];

    if (isChecked) {

        calculatedPipelineNames.forEach(function(pipelineName) {
            if (pipelineName !== layerName) {

                var otherCheckbox = document.querySelector(`input[type="checkbox"]`);
                var labels = document.querySelectorAll('label');
                labels.forEach(function(label) {
                    if (label.textContent.trim().includes(pipelineName)) {
                        var cb = label.querySelector('input[type="checkbox"]');
                        if (cb) {
                            cb.checked = false;

                            var otherSourceId = pipelineName.toLowerCase().replace(/\\s+/g, '-');
                            try {
                                map.setLayoutProperty(otherSourceId + '-casing', 'visibility', 'none');
                                map.setLayoutProperty(otherSourceId + '-core', 'visibility', 'none');
                            } catch(e) {
                                console.warn('[MUTUAL_EXCLUSIVE] Failed to hide', pipelineName);
                            }
                        }
                    }
                });
            }
        });


        layerIds.forEach(function(layerId) {
            try {
                map.setLayoutProperty(layerId, 'visibility', 'visible');
            } catch(e) {
                console.warn('[CONTROL] Layer not found:', layerId);
            }
        });

        window.activeCalculatedPipeline = layerName;
        console.log('[MUTUAL_EXCLUSIVE] ✓ Active pipeline:', layerName);

    } else {

        layerIds.forEach(function(layerId) {
            try {
                map.setLayoutProperty(layerId, 'visibility', 'none');
            } catch(e) {
                console.warn('[CONTROL] Layer not found:', layerId);
            }
        });

        if (window.activeCalculatedPipeline === layerName) {
            window.activeCalculatedPipeline = null;
        }
        console.log('[MUTUAL_EXCLUSIVE] ✓ Deactivated pipeline:', layerName);
    }
}


function getGroupKeyForLayer(layerName) {
    var groupMappings = {
        'WWTP Outlines': 'wwtp',
        'Available Area': 'wwtp',
        'EHB Transmission': 'gasTransmission',
        'EHB High Pressure': 'gasTransmission', 
        'Gas Pipelines': 'gasTransmission',
        'Pipeline to EHB': 'calculatedPipelines',
        'Pipeline to Gaspipelines': 'calculatedPipelines',
        'OpenInfraMap Energy': 'energy',
        'Protected Areas (NatDB)': 'protectedAreas',
        'Protected Areas (Natura 2000)': 'protectedAreas'
    };

    return groupMappings[layerName] || null;
}






""",

            # MapLibre doesn't use panes - layers are managed by order

            # Helper: GeoJSON
            "function add(url,style,dofit,name,visible){fetch(url).then(r=>r.json()).then(g=>{var lyr=L.geoJSON(g,style||{});overlays[name]=lyr;layerControl.addOverlay(lyr,name);if(visible!==false)lyr.addTo(map);if(dofit){try{map.fitBounds(lyr.getBounds(),{maxZoom:17});}catch(e){}};groupLayers();});}",
            # Helper: Pipeline-Optik (Mantel+Kern, neutral)
            "function addPipe(url,name,visible,casingColor,coreColor){fetch(url).then(r=>r.json()).then(g=>{var a=L.geoJSON(g,{style:()=>({color:casingColor,weight:6,opacity:0.9,lineCap:'round',lineJoin:'round'})});var b=L.geoJSON(g,{style:()=>({color:coreColor,weight:3,opacity:1.0,lineCap:'round',lineJoin:'round'})});var grp=L.layerGroup([a,b]);overlays[name]=grp;layerControl.addOverlay(grp,name);if(visible!==false)grp.addTo(map);groupLayers();});}",
            # Helper: ArcGIS Dynamic
            "function addEsriDynamic(url,name,visible,pane){var lyr=L.esri.dynamicMapLayer({url:url,pane:pane,opacity:1});overlays[name]=lyr;layerControl.addOverlay(lyr,name);if(visible===true)lyr.addTo(map);groupLayers();}",

            # Gruppierung ohne Checkboxen am Gruppen-Titel
            "var _sepDone=false;",
            "function groupLayers(){",
            "  var list=document.querySelector('.leaflet-control-layers-overlays'); if(!list) return;",
            "  list.querySelectorAll('.grp,.grp-sep').forEach(function(el){el.remove();});",
            "  function createGroup(title,id){var g=document.createElement('div');g.className='grp';g.setAttribute('data-group',id);var h=document.createElement('div');h.className='grp-h';h.textContent=title;g.appendChild(h);var items=document.createElement('div');items.className='grp-items';g.appendChild(items);list.appendChild(g);return items;}",
            "  var ww = createGroup('WWTP','grp_wwtp');",
            "  var pa = createGroup('Protected Areas','grp_pa');",
            "  var dn = createGroup('Distribution Networks','grp_dist');",
            "  var cp = createGroup('Calculated Pipelines','grp_calc');",
            "  var en = createGroup('Energy','grp_energy');",
            "  function move(name, container){var lab=[...list.querySelectorAll('label')].find(l=>{var s=l.querySelector('span');return s&&s.textContent.trim()===name});if(lab) container.appendChild(lab);}",
            "  move('Outlines', ww);",
            "  move('Available Area', ww);",
            "  move('Protected Areas (NatDB)', pa);",
            "  move('Protected Areas (Natura 2000)', pa);",
            "  move('EHB Transmission', dn);",
            "  move('EHB High Pressure', dn);",
            "  move('Gas Pipelines', dn);",
            "  move('Pipeline to EHB', cp);",
            "  move('Pipeline to Gaspipelines', cp);",
            "  move('OpenInfraMap Energy', en);",
            "  if(!_sepDone){var labs=[...list.querySelectorAll('label')];var esri=labs.find(l=>{var s=l.querySelector('span');return s&&s.textContent.trim()==='Esri Reference'});if(esri){var sep=document.createElement('div');sep.className='grp-sep';esri.insertAdjacentElement('afterend',sep);_sepDone=true;}}",
            "}",
            "function fitTo(url){fetch(url).then(r=>r.json()).then(g=>{try{var t=L.geoJSON(g);map.fitBounds(t.getBounds(),{maxZoom:17});}catch(e){}});}",
        ]

        # --- HTML fertigstellen & ausliefern ---
        maplibre_html += ["</script></body></html>"]
        html_doc = "\n".join(maplibre_html)
        payloads["/index.html"] = html_doc

        try:
            base_url, srv, th = _start_geojson_server(payloads)
            import webbrowser
            if not getattr(self, "_map_tab_opened", False):
                print('[MAP] URL:', base_url + '/index.html')
                webbrowser.open(base_url + '/index.html', new=2)
                setattr(self, "_map_tab_opened", True)
        except Exception:
            pass
        finally:
            setattr(self, "_map_opening_now", False)

    def _make_layers_icon(self, w=22, h=22):
        """Erzeugt ein kleines 'Layers'-Icon (drei gestapelte Rauten)."""
        im = Image.new("RGBA", (w, h), (0, 0, 0, 0))
        d = ImageDraw.Draw(im)

        def rhomb(cx, cy, sx, sy, fill, outline):
            pts = [(cx, cy - sy), (cx + sx, cy), (cx, cy + sy), (cx - sx, cy)]
            d.polygon(pts, fill=fill, outline=outline)

        # dezente Konturen + eine blaue mittlere Fläche
        rhomb(w // 2, h // 2 - 5, 7, 4, (0, 0, 0, 0), (60, 60, 60, 220))
        rhomb(w // 2, h // 2, 7, 4, (80, 120, 200, 180), (50, 80, 150, 255))
        rhomb(w // 2, h // 2 + 5, 7, 4, (0, 0, 0, 0), (60, 60, 60, 200))
        return ImageTk.PhotoImage(im)

    def _open_map_menu(self):
        """Öffnet das Dropdown unter dem Layers-Icon."""
        m = tk.Menu(self, tearoff=False)
        m.add_command(
            label=("Hide Outlines" if self._overlay_visible else "Show Outlines"),
            command=self._toggle_overlay
        )
        m.add_separator()
        m.add_command(label="Open Esri Map", command=self._open_esri_map)
        try:
            x = self._menu_btn.winfo_rootx()
            y = self._menu_btn.winfo_rooty() + self._menu_btn.winfo_height()
            m.tk_popup(x, y)
        finally:
            try:
                m.grab_release()
            except Exception:
                pass

    def _toggle_pipeline(self):
        # Nur GUI-State;
        self._pipeline_visible = not getattr(self, "_pipeline_visible", False)
        log("[GUI] Pipeline layer:", "ON" if self._pipeline_visible else "OFF")

    def _toggle_free_area(self):
        # Nur GUI-State;
        self._free_area_visible = not getattr(self, "_free_area_visible", False)
        log("[GUI] Free Area layer:", "ON" if self._free_area_visible else "OFF")

    def _toggle_overlay(self):
        self._overlay_visible = not getattr(self, "_overlay_visible", False)
        self._update_map_image()


# ------------------------- Start -------------------------
if __name__ == "__main__":
    app = App();
    app.mainloop()
