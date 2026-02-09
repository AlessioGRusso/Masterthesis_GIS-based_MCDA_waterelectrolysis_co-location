from __future__ import annotations

from pathlib import Path
import os
import sys
import warnings
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.errors import ShapelyDeprecationWarning
from pyproj import CRS


warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

try:
    import fiona  # optional (für layer auto-detect)
except Exception:
    fiona = None


# ---------------- Konfiguration ----------------
ROOT = Path(__file__).resolve().parent

EXCEL_PATH = ROOT / "Output" / "UWWTD_TP_Database.xlsx"
MAIN_SHEET = "General Data"

ID_COL  = "UWWTD Code"
LAT_COL = "Latitude"
LON_COL = "Longitude"
COORD_EPSG = 4326

FREE_AREA_GPKG = ROOT / "Daten" / "WWTP_Free_Area.gpkg"
FREE_AREA_LAYER = None          # None = autodetect

PLANT_POLYS_GPKG = ROOT / "Output" / "WWTP Geopackages" / "WWTPS_Shapes.gpkg"
PLANT_POLYS_LAYER = None        # None = autodetect (optional)

RADIUS_M = 1000.0               # 1km Suchradius für verfügbare Fläche
DROP_INVALID_COORDS = False     # True = invalid coords werden rausgeworfen

OUT_GPKG_DIR   = ROOT / "Output" / "WWTP Geopackages"
OUT_GPKG_FILE  = OUT_GPKG_DIR / "WWTPS_nofit_after_AAF.gpkg"
OUT_GPKG_LAYER = "WWTPS_nofit_after_AAF"

ADDITIONAL_SHEET = "Additional Data"


# ---------------- Helpers ----------------
def pick_polygon_layer(gpkg: Path, wanted: Optional[str] = None) -> Optional[str]:
    """Nimmt gewünschtes Layer oder versucht ein Polygon-Layer zu erraten."""
    if wanted:
        return wanted
    if fiona is None or not gpkg.exists():
        return None

    try:
        layers = fiona.listlayers(gpkg)
    except Exception:
        return None

    for lyr in layers:
        try:
            g = gpd.read_file(gpkg, layer=lyr, rows=1)
            if not g.empty and any("Polygon" in str(t) for t in g.geom_type.unique()):
                return lyr
        except Exception:
            continue

    return layers[0] if layers else None


def ensure_points(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """Macht aus Lat/Lon eine Point-GeoDataFrame (WGS84)."""
    for c in (LAT_COL, LON_COL):
        if c not in df.columns:
            raise KeyError(f"Spalte '{c}' fehlt im Sheet '{MAIN_SHEET}'.")

    lat = pd.to_numeric(df[LAT_COL], errors="coerce")
    lon = pd.to_numeric(df[LON_COL], errors="coerce")
    geom = [Point(x, y) if np.isfinite(x) and np.isfinite(y) else None for x, y in zip(lon, lat)]
    return gpd.GeoDataFrame(df.copy(), geometry=geom, crs=f"EPSG:{COORD_EPSG}")


def choose_metric_crs(polys_crs, pts: gpd.GeoDataFrame) -> str:
    """Nimmt metrisches CRS: wenn Polygone schon in Meter sind → das; sonst local AEQD."""
    if polys_crs:
        c = CRS(polys_crs)
        if c.is_projected:
            # wenn projected, nehmen wir’s (in der Praxis: meistens Meter)
            return str(polys_crs)

    pts_wgs = pts.to_crs(4326)
    lon0 = float(pts_wgs.geometry.x.mean())
    lat0 = float(pts_wgs.geometry.y.mean())
    return CRS.from_proj4(
        f"+proj=aeqd +lat_0={lat0} +lon_0={lon0} +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    ).to_string()


def infer_binary_class_column(polys: gpd.GeoDataFrame) -> Optional[str]:
    """Sucht eine Spalte, die wie 0/1/NaN aussieht (optional)."""
    for col in polys.columns:
        if col == "geometry":
            continue
        s = polys[col]
        if s.isna().all():
            continue
        try:
            vals = set(pd.unique(s.dropna().astype(float).round().astype(int)))
            if vals <= {0, 1}:
                return col
        except Exception:
            pass
    return None


def class_label(v) -> str:
    """Mapping wie im Original: 0=Free Ground, 1=Forest, NaN/Müll=Mixed."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "Mixed"
    try:
        iv = int(float(v))
        return "Free Ground" if iv == 0 else "Forest" if iv == 1 else "Mixed"
    except Exception:
        return "Mixed"


def load_excel_all_sheets(path: Path) -> Tuple[Dict[str, pd.DataFrame], list[str]]:
    with pd.ExcelFile(path) as xls:
        order = xls.sheet_names
        dfs = {sh: pd.read_excel(xls, sheet_name=sh) for sh in order}
    return dfs, order


def load_polygons(gpkg: Path, layer: Optional[str]) -> gpd.GeoDataFrame:
    lyr = pick_polygon_layer(gpkg, layer)
    gdf = gpd.read_file(gpkg, layer=lyr)
    gdf = gdf[gdf.geometry.notna()]
    if gdf.empty:
        raise ValueError(f"Keine gültigen Polygongeometrien in {gpkg.name}.")
    return gdf


def match_plant_polys_to_main(
    plant_polys: gpd.GeoDataFrame, main_df: pd.DataFrame
) -> gpd.GeoDataFrame:
    """Sortiert/Matched Plant-Polys auf Reihenfolge von main_df (über ID_COL), wenn möglich."""
    if ID_COL not in main_df.columns or ID_COL not in plant_polys.columns:
        return plant_polys

    d = {str(code): geom for code, geom in zip(plant_polys[ID_COL], plant_polys.geometry)}
    geoms = [d.get(str(code)) for code in main_df[ID_COL]]
    return gpd.GeoDataFrame(main_df[[ID_COL]].copy(), geometry=geoms, crs=plant_polys.crs)


def compute_metrics(
    pts_wgs: gpd.GeoDataFrame,
    free_polys: gpd.GeoDataFrame,
    radius_m: float,
    plant_polys: Optional[gpd.GeoDataFrame] = None,
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    Gibt zurück:
      - nearest_dist_m: Distanz zum nächsten Polygon (oder 0 bei Überschneidung via plant_polys)
      - sum_area_m2: Summe Polygonfläche im Radius um den Punkt
      - nearest_class: Label (Free Ground/Forest/Mixed)
    """
    metric = choose_metric_crs(free_polys.crs, pts_wgs)
    pts = pts_wgs.to_crs(metric)
    polys = free_polys.to_crs(metric).copy()
    polys = polys[polys.geometry.notna()]

    class_col = infer_binary_class_column(polys)
    if class_col is None:
        polys["__class__"] = np.nan
        class_col = "__class__"

    # Distanz + Klasse: bevorzugt über Anlagen-Polygone, falls vorhanden
    if plant_polys is not None and not plant_polys.empty:
        plants = plant_polys.to_crs(metric)
        plants = plants[plants.geometry.notna()]

        near = gpd.sjoin_nearest(
            plants, polys[[class_col, "geometry"]],
            how="left", distance_col="__dist_m"
        )

        nearest_class = (
            near.groupby(near.index)[class_col].first()
            .apply(class_label)
            .reindex(pts.index)
            .fillna("Mixed")
        )
        nearest_dist = (
            near.groupby(near.index)["__dist_m"].first()
            .astype(float)
            .reindex(pts.index)
            .fillna(np.inf)
        )

        # Überschneidung? dann Distanz = 0
        overlaps = gpd.sjoin(plants, polys[["geometry"]], how="left", predicate="intersects")
        has_overlap = overlaps.groupby(overlaps.index).size() > 0
        nearest_dist.loc[has_overlap.reindex(pts.index).fillna(False)] = 0.0

    else:
        near = gpd.sjoin_nearest(
            pts, polys[[class_col, "geometry"]],
            how="left", distance_col="__dist_m"
        )
        nearest_class = near[class_col].apply(class_label).reindex(pts.index).fillna("Mixed")
        nearest_dist = near["__dist_m"].astype(float).reindex(pts.index)

    # Summe Flächen im Radius
    polys["__area_m2"] = polys.geometry.area

    if radius_m <= 0:
        j = gpd.sjoin(pts, polys[["__area_m2", "geometry"]], how="left", predicate="within")
        sum_area = j.groupby(j.index)["__area_m2"].sum().reindex(pts.index).fillna(0.0)
    else:
        buf = pts.copy()
        buf["geometry"] = buf.geometry.buffer(radius_m)
        j = gpd.sjoin(buf, polys[["__area_m2", "geometry"]], how="left", predicate="intersects")
        sum_area = j.groupby(j.index)["__area_m2"].sum().reindex(pts.index).fillna(0.0)

    return nearest_dist, sum_area, nearest_class


def sync_sheets_to_general(
    sheets: Dict[str, pd.DataFrame], sheet_order: list[str]
) -> None:
    """Stellt sicher: alle Sheets enthalten nur IDs, die im MAIN_SHEET existieren."""
    if MAIN_SHEET not in sheets or ID_COL not in sheets[MAIN_SHEET].columns:
        return

    general_codes = set(sheets[MAIN_SHEET][ID_COL].astype(str).str.strip())
    for sh in sheet_order:
        if sh == MAIN_SHEET:
            continue
        df = sheets[sh]
        if ID_COL in df.columns:
            before = len(df)
            sheets[sh] = df[df[ID_COL].astype(str).str.strip().isin(general_codes)].copy()
            after = len(sheets[sh])
            if before != after:
                print(f"  → {sh}: {before} → {after} Zeilen (sync mit General Data)")


# ---------------- Main ----------------
def main() -> None:
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(f"Excel nicht gefunden: {EXCEL_PATH}")
    if not FREE_AREA_GPKG.exists():
        raise FileNotFoundError(f"GPKG nicht gefunden: {FREE_AREA_GPKG}")

    sheets, sheet_order = load_excel_all_sheets(EXCEL_PATH)
    if MAIN_SHEET not in sheets:
        raise KeyError(f"Sheet '{MAIN_SHEET}' nicht gefunden. Verfügbar: {sheet_order}")

    df_main = sheets[MAIN_SHEET]
    pts = ensure_points(df_main)

    free_polys = load_polygons(FREE_AREA_GPKG, FREE_AREA_LAYER)

    plant_polys = None
    if PLANT_POLYS_GPKG.exists():
        try:
            plant_polys_raw = load_polygons(PLANT_POLYS_GPKG, PLANT_POLYS_LAYER)
            plant_polys = match_plant_polys_to_main(plant_polys_raw, df_main)
            print(f"✓ Plant polygons geladen: {PLANT_POLYS_GPKG.name}")
        except Exception as e:
            print(f"⚠️ Plant polygons konnten nicht geladen werden: {e}")

    nearest_dist_m, sum_area_m2, nearest_class = compute_metrics(
        pts, free_polys, RADIUS_M, plant_polys
    )

    invalid_coords = pts.geometry.isna()
    has_nearby = sum_area_m2 > 0
    keep_mask = (has_nearby & ~invalid_coords) if DROP_INVALID_COORDS else (has_nearby | invalid_coords)

    total_before = len(df_main)
    total_kept = int(keep_mask.sum())
    print(f"✓ AAF: {total_kept} von {total_before} Anlagen behalten (Radius: {RADIUS_M} m)")

    # ---- Excel schreiben (neu, sauber) ----
    tmp = EXCEL_PATH.with_name(EXCEL_PATH.stem + "__tmp_write.xlsx")
    ids_to_keep = set(df_main.loc[keep_mask, ID_COL].astype(str)) if ID_COL in df_main.columns else None

    try:
        filtered: Dict[str, pd.DataFrame] = {}
        main_drop_idx = df_main.index[~keep_mask]

        for sh in sheet_order:
            df = sheets[sh]
            if ids_to_keep is not None and ID_COL in df.columns:
                filtered[sh] = df[df[ID_COL].astype(str).isin(ids_to_keep)].copy()
            else:
                # fallback: wenn sheet gleiche länge wie main, drop nach index
                filtered[sh] = df.drop(index=main_drop_idx).copy() if len(df) == len(df_main) else df.copy()

        # Sync IDs (wie im Original)
        sync_sheets_to_general(filtered, sheet_order)

        # Additional Data (nur kept)
        add = pd.DataFrame({
            ID_COL: df_main.loc[keep_mask, ID_COL].values if ID_COL in df_main.columns else "",
            "Name": df_main.loc[keep_mask, "Name"].values if "Name" in df_main.columns else "",
            "Nearest Distance [m]": nearest_dist_m.loc[keep_mask].astype(float).round(2).values,
            "Total Sum Available Area [m²]": sum_area_m2.loc[keep_mask].astype(float).round(2).values,
            "Nearest Area Class": nearest_class.loc[keep_mask].values,
        })
        filtered[ADDITIONAL_SHEET] = add

        with pd.ExcelWriter(tmp, engine="openpyxl") as xw:
            for sh in sheet_order:
                filtered[sh].to_excel(xw, sheet_name=sh, index=False)
            # Additional Data ans Ende (auch wenn nicht in sheet_order)
            filtered[ADDITIONAL_SHEET].to_excel(xw, sheet_name=ADDITIONAL_SHEET, index=False)

        os.replace(tmp, EXCEL_PATH)
        print(f"✓ Excel aktualisiert: {EXCEL_PATH.name}")

    except Exception as e:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass
        print(
            "Fehler beim Schreiben der Excel (evtl. Datei geöffnet?). "
            f"Details: {e}",
            file=sys.stderr,
        )
        sys.exit(2)

    # ---- GPKG: rausgefilterte Anlagen als Punkte ----
    OUT_GPKG_DIR.mkdir(parents=True, exist_ok=True)

    removed = pts.loc[~keep_mask].copy()
    removed["Nearest Distance [m]"] = nearest_dist_m.loc[~keep_mask].astype(float).round(2).values
    removed["Total Sum Available Area [m²]"] = sum_area_m2.loc[~keep_mask].astype(float).round(2).values
    removed["Nearest Area Class"] = nearest_class.loc[~keep_mask].values
    removed = removed.to_crs(4326)

    if OUT_GPKG_FILE.exists():
        try:
            OUT_GPKG_FILE.unlink()
        except Exception:
            pass

    removed.to_file(OUT_GPKG_FILE, layer=OUT_GPKG_LAYER, driver="GPKG")
    print(f"✓ GPKG geschrieben: {OUT_GPKG_FILE.name} ({len(removed)} Anlagen ohne passende Fläche)")


if __name__ == "__main__":
    main()
