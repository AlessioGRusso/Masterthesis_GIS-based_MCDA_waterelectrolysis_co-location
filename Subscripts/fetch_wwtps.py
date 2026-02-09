#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import geopandas as gpd
import pandas as pd
import fiona
from shapely.geometry import Point, Polygon, MultiPolygon
import osmnx as ox

# ===== Konfiguration =====
SEARCH_RADIUS_M = 2000
REQUEST_SLEEP_S = 1.0
OVERPASS_TIMEOUT_S = 120
INCLUDE_POINT_BUFFERS = True
POINT_BUFFER_M = 50
OUTPUT_LAYER = "klaeranlagen_osm"

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
]

ID_COL_CANDIDATES = ["plant_id", "id", "ID", "uuid", "uid"]
NAME_COL_CANDIDATES = ["name", "Name", "plant_name", "WWTP_NAME", "wwtp_name"]


def project_root_from_this_file() -> Path:
    return Path(__file__).resolve().parents[1]


def center_of_geom(geom):
    if isinstance(geom, (Polygon, MultiPolygon)):
        return geom.representative_point()
    if isinstance(geom, Point):
        return geom
    return geom.centroid


def set_overpass():
    ox.settings.log_console = False
    ox.settings.use_cache = True
    ox.settings.overpass_settings = f"[out:json][timeout:{OVERPASS_TIMEOUT_S}]"
    ox.settings.overpass_endpoint = OVERPASS_ENDPOINTS[0]


def _deg_per_meter(lat: float) -> tuple[float, float]:
    # grobe (sehr gute) Umrechnung Meter -> Grad
    deg_per_m_lat = 1.0 / 111320.0
    deg_per_m_lon = 1.0 / (111320.0 * max(1e-6, math.cos(math.radians(lat))))
    return deg_per_m_lat, deg_per_m_lon


def _bbox_from_point(lat: float, lon: float, dist_m: int) -> tuple[float, float, float, float]:
    dlat, dlon = _deg_per_meter(lat)
    d_lat = dist_m * dlat
    d_lon = dist_m * dlon
    north = lat + d_lat
    south = lat - d_lat
    east = lon + d_lon
    west = lon - d_lon
    return north, south, east, west


def _features_from_point_or_bbox(lat: float, lon: float, radius_m: int, tags: dict) -> gpd.GeoDataFrame:
    """
    Versionstoleranter Wrapper:
    - OSMnx >= 2.x: features_from_point
    - OSMnx 1.x: geometries_from_point
    - Fallback: *_from_bbox
    """
    # bevorzugt Punktabfrage
    if hasattr(ox, "features_from_point"):
        return ox.features_from_point((lat, lon), tags=tags, dist=radius_m)
    if hasattr(ox, "geometries_from_point"):
        return ox.geometries_from_point((lat, lon), tags=tags, dist=radius_m)

    # Fallback per BBox
    north, south, east, west = _bbox_from_point(lat, lon, radius_m)
    if hasattr(ox, "features_from_bbox"):
        return ox.features_from_bbox(north, south, east, west, tags=tags)
    if hasattr(ox, "geometries_from_bbox"):
        return ox.geometries_from_bbox(north, south, east, west, tags=tags)

    raise AttributeError("Deine OSMnx-Version stellt weder *_from_point noch *_from_bbox bereit.")


def fetch_wwtp_candidates(lat: float, lon: float, radius_m: int) -> gpd.GeoDataFrame:
    tags = {"man_made": "wastewater_plant"}
    last_err = None
    for ep in OVERPASS_ENDPOINTS:
        ox.settings.overpass_endpoint = ep
        try:
            return _features_from_point_or_bbox(lat, lon, radius_m, tags)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"Overpass-Fehler nach allen Endpoints: {last_err}")


def best_match_for_source(src_geom, candidates: gpd.GeoDataFrame) -> Optional[gpd.GeoDataFrame]:
    if candidates.empty:
        return None

    cand = candidates[candidates.geometry.geom_type.isin(["Polygon", "MultiPolygon", "Point"])].copy()
    if cand.empty:
        return None

    pt = center_of_geom(src_geom)
    lon, lat = pt.x, pt.y
    zone = int((lon + 180) // 6) + 1
    epsg = 32600 + zone if lat >= 0 else 32700 + zone

    if INCLUDE_POINT_BUFFERS and any(cand.geometry.geom_type == "Point"):
        cand_proj = cand.to_crs(epsg)
        is_pt = cand_proj.geometry.geom_type == "Point"
        cand_proj.loc[is_pt, "geometry"] = cand_proj.loc[is_pt, "geometry"].buffer(POINT_BUFFER_M)
        cand = cand_proj.to_crs(4326)
    else:
        cand = cand[cand.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()

    if cand.empty:
        return None

    src_proj = gpd.GeoSeries([src_geom], crs=4326).to_crs(epsg).iloc[0]
    cand_proj = cand.to_crs(epsg)

    if isinstance(src_geom, (Polygon, MultiPolygon)):
        inter = cand_proj.geometry.intersection(src_proj)
        cand_proj["overlap_area"] = inter.area
        cand_proj["overlap_ratio"] = cand_proj["overlap_area"] / cand_proj.geometry.area.replace(0, 1)
        cand_proj = cand_proj.sort_values(["overlap_ratio", "overlap_area"], ascending=False)
        top = cand_proj.iloc[0]
        if top["overlap_area"] <= 0:
            cand_proj["dist"] = cand_proj.geometry.distance(src_proj)
            top = cand_proj.sort_values("dist", ascending=True).iloc[0]
    else:
        cand_proj["dist"] = cand_proj.geometry.distance(src_proj)
        top = cand_proj.sort_values("dist", ascending=True).iloc[0]

    return cand.loc[[top.name]]


def pick_first_existing(cols: List[str], gdf: gpd.GeoDataFrame) -> Optional[str]:
    for c in cols:
        if c in gdf.columns:
            return c
    return None


def choose_layer(path: Path) -> str:
    layers = fiona.listlayers(path)
    pref = [l for l in layers if any(k in l.lower() for k in ["plant", "wwtp", "treatment"])]
    return pref[0] if pref else layers[0]


def main():
    print("== WWTP OSM-Automat (fixe Eingabe) ==")
    print(f"Erkannte OSMnx-Version: {getattr(ox, '__version__', 'unbekannt')}")
    proj_root = project_root_from_this_file()

    in_path = proj_root / "Output" / "WWTP Geopackages" / "WWTPS_after_AAF.gpkg"
    if not in_path.exists():
        raise FileNotFoundError(f"Eingabe-GPKG nicht gefunden: {in_path}")

    in_layer = choose_layer(in_path)
    print(f"Eingabe: {in_path.name}  |  Layer: {in_layer}")

    gdf_src = gpd.read_file(in_path, layer=in_layer)
    if gdf_src.crs is None:
        raise ValueError("Eingabe-GPKG hat kein CRS. Bitte CRS (z. B. EPSG:4326) setzen.")
    if gdf_src.crs.to_epsg() != 4326:
        gdf_src = gdf_src.to_crs(4326)
    gdf_src = gdf_src[~gdf_src.geometry.is_empty & gdf_src.geometry.notnull()].copy()
    if gdf_src.empty:
        raise ValueError("Eingabe-GPKG enthält keine gültigen Geometrien.")

    id_col = pick_first_existing(ID_COL_CANDIDATES, gdf_src)
    name_col = pick_first_existing(NAME_COL_CANDIDATES, gdf_src)
    if id_col:
        print(f"ID-Spalte erkannt: {id_col}")
    if name_col:
        print(f"Name-Spalte erkannt: {name_col}")

    set_overpass()

    results = []
    seen = set()

    for i, row in gdf_src.iterrows():
        src_geom = row.geometry
        pt = center_of_geom(src_geom)
        lat, lon = pt.y, pt.x

        try:
            cand = fetch_wwtp_candidates(lat, lon, SEARCH_RADIUS_M)
        except Exception as e:
            print(f"[{i}] Overpass-Fehler bei ({lat:.5f}, {lon:.5f}): {e}")
            time.sleep(REQUEST_SLEEP_S)
            continue

        if cand is None or cand.empty:
            print(f"[{i}] Keine Kandidaten im Umkreis {SEARCH_RADIUS_M} m.")
            time.sleep(REQUEST_SLEEP_S)
            continue

        cand = cand.reset_index()  # (element_type, osmid) → Spalten
        best = best_match_for_source(src_geom, cand)
        if best is None or best.empty:
            print(f"[{i}] Keine passende Fläche identifiziert.")
            time.sleep(REQUEST_SLEEP_S)
            continue

        key_cols = [c for c in ["element_type", "osmid"] if c in best.columns]
        if key_cols:
            key = tuple(best.iloc[0][k] for k in key_cols)
            if key in seen:
                time.sleep(REQUEST_SLEEP_S)
                continue
            seen.add(key)

        keep = [c for c in ["name", "operator", "start_date", "ref", "website", "element_type", "osmid"] if c in best.columns]
        out = best[keep + ["geometry"]].copy()

        if id_col:
            out["source_id"] = row[id_col]
        if name_col:
            out["source_name"] = row[name_col]

        results.append(out)
        print(f"[{i}] Treffer übernommen (aktuell: {sum(len(r) for r in results)})")
        time.sleep(REQUEST_SLEEP_S)

    if not results:
        print("Keine Umrisse gefunden – keine Ausgabe geschrieben.")
        return

    gdf_out = pd.concat(results, ignore_index=True).set_crs(4326, allow_override=True)

    stamp = datetime.now().strftime("%Y%m%d")
    out_path = proj_root / "Output" / "WWTP Geopackages" / f"WWTPS_after_DF_OSM_{stamp}.gpkg"
    gdf_out.to_file(out_path, layer=OUTPUT_LAYER, driver="GPKG")
    print(f"FERTIG ✔  Gespeichert: {out_path}  |  Layer: {OUTPUT_LAYER}  |  Features: {len(gdf_out)}")


if __name__ == "__main__":
    main()

