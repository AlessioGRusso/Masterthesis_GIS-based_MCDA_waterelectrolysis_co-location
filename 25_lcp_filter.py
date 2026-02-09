
import ast
import importlib.util
import os
import re
import time
import shutil
from pathlib import Path
from typing import Callable, Tuple, Set

import numpy as np
import pandas as pd

# Konfiguration - hier kann man die wichtigsten Einstellungen ändern
GEN_SHEET = "General Data"
PIPE_SHEET_CANDIDATES = ["H2 Logistics", "Pipeline Connection", "Pipeline connection"]
SCENARIOS = [
    ("EHB", "LeastCostLine (EHB) [km]", "Built Scenario 1 (EHB)"),
]
PE_COL = "Capacity/PE"
KEY_COL = "UWWTD Code"
DIST_FACTOR = 1.0

# Hilfsfunktionen für Dateipfade und Projektstruktur
def find_project_root(start: Path) -> Path:
    """Findet das Projekt-Root-Verzeichnis"""
    cur = start.resolve()
    for _ in range(10):
        if (cur / "Output").exists() or (cur / "Subscripts").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return start.resolve()

def find_excel(base: Path) -> Path:
    """Findet die neueste Excel-Datei im Output-Ordner"""
    out = base / "Output"
    if not out.exists():
        raise FileNotFoundError(f"Ordner 'Output' nicht gefunden: {out}")
    cands = []
    for pat in ("UWWTD_TP_Database*.xlsx", "UWWTD_TP_Database*.xls"):
        cands += list(out.glob(pat))
    if not cands:
        raise FileNotFoundError(f"Keine Excel gefunden (UWWTD_TP_Database*.xls*) in {out}")
    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0]

def file_has_decision_fn(py_path: Path) -> bool:
    """Prüft ob eine Python-Datei eine Entscheidungsfunktion hat"""
    try:
        tree = ast.parse(py_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    return any(isinstance(n, ast.FunctionDef) and n.name == "decision" for n in ast.walk(tree))

def import_decision_fn(py_path: Path) -> Callable[[float, float], Tuple[str, float]]:
    spec = importlib.util.spec_from_file_location(py_path.stem, str(py_path))
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    if not hasattr(mod, "decision"):
        raise AttributeError(f"In {py_path} keine Funktion 'decision(distance_km, pe)' gefunden.")
    return getattr(mod, "decision")

def find_decision_script(base: Path) -> Path:
    preferred = base / "electrolysis_decision.py"
    if preferred.exists():
        return preferred
    cands = []
    for folder in (base / "Subscripts", base):
        if folder.exists():
            for p in folder.glob("*.py"):
                if file_has_decision_fn(p):
                    cands.append(p)
    if not cands:
        raise FileNotFoundError(
            "Kein Python-Script mit decision(distance_km, pe) gefunden. "
            "Lege es als 'electrolysis_decision.py' ins Projekt-Root oder in 'Subscripts/'."
        )
    cands.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0]

def safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return np.nan

def atomic_replace(src: Path, dst: Path, attempts: int = 6, backoff: float = 0.5) -> None:
    """
    Wiederholtes os.replace für Windows-Dateisperren.
    Letzter Versuch: Backup von dst -> dann replace.
    """
    last_err: Exception | None = None
    for i in range(attempts):
        try:
            os.replace(src, dst)
            return
        except PermissionError as e:
            last_err = e
            time.sleep(backoff * (i + 1))
    # letzter Rettungsversuch mit Backup
    try:
        bak = dst.with_suffix(dst.suffix + ".bak")
        shutil.copy2(dst, bak)
        os.replace(src, dst)
        return
    except Exception as e:
        # ursprünglichen WinError 5 anzeigen, wenn vorhanden
        if last_err:
            raise last_err
        raise PermissionError(f"Could not replace {dst}: {e}")

# ---- Kernlogik ----

def write_deleted_to_gpkg(root: Path, gen_df: pd.DataFrame, to_remove: Set[str]) -> None:
    """
    Schreibt ein GeoPackage mit allen gelöschten Anlagen in
    Output/WWTP Geopackages/WWTPS_notfit_after_LCPF.gpkg.
    Erkennt typische Koordinatenspalten automatisch.
    """
    try:
        import geopandas as gpd
    except ImportError:
        print("[!] 'geopandas' ist nicht installiert. Bitte installieren mit:")
        print("    pip install geopandas pyogrio shapely pyproj")
        return

    if not to_remove:
        print("[i] Keine gelöschten Anlagen; GPKG wird nicht erzeugt.")
        return

    cols_lower = {c.lower(): c for c in gen_df.columns}
    lon_names = ["lon", "longitude", "long", "x", "coord_x", "easting"]
    lat_names = ["lat", "latitude", "y", "coord_y", "northing"]

    lon_col = next((cols_lower[n] for n in lon_names if n in cols_lower), None)
    lat_col = next((cols_lower[n] for n in lat_names if n in cols_lower), None)

    df = gen_df.copy()
    df = df[df[KEY_COL].astype(str).isin(to_remove)].copy()

    out_dir = root / "Output" / "WWTP Geopackages"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "WWTPS_nofit_after_LCPF.gpkg"

    if lon_col is None or lat_col is None:
        csv_path = out_dir / "WWTPS_notfit_after_LCPF.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"[!] Keine Koordinatenspalten erkannt – CSV statt GPKG geschrieben: {csv_path}")
        return

    df["_lon"] = pd.to_numeric(df[lon_col], errors="coerce")
    df["_lat"] = pd.to_numeric(df[lat_col], errors="coerce")
    df = df.dropna(subset=["_lon", "_lat"])

    if df.empty:
        print("[!] Keine gültigen Koordinatenwerte für gelöschte Anlagen – GPKG wird nicht erzeugt.")
        return

    gdf = gpd.GeoDataFrame(
        df.drop(columns=["_lon", "_lat"]),
        geometry=gpd.points_from_xy(df["_lon"], df["_lat"]),
        crs="EPSG:4326",
    )

    try:
        gdf.to_file(out_path, layer="notfit_after_LCPF", driver="GPKG")
        print(f"[i] GPKG geschrieben: {out_path}")
    except Exception as e:
        csv_path = out_dir / "WWTPS_notfit_after_LCPF.csv"
        df.drop(columns=gdf.geometry.name, errors="ignore").to_csv(csv_path, index=False, encoding="utf-8")
        print(f"[!] Konnte GPKG nicht schreiben ({e}). CSV statt GPKG geschrieben: {csv_path}")


def run() -> None:
    here = Path(__file__).resolve().parent
    root = find_project_root(here)

    excel_path = find_excel(root)
    decision_path = find_decision_script(root)

    print(f"[i] Projekt-Root: {root}")
    print(f"[i] Excel:       {excel_path.name}")
    try:
        print(f"[i] decision.py: {decision_path.relative_to(root)}")
    except Exception:
        print(f"[i] decision.py: {decision_path}")

    decision = import_decision_fn(decision_path)

    # Ganze Mappe in den RAM lesen (keine offenen Handles)
    book = pd.read_excel(excel_path, sheet_name=None)
    sheets = list(book.keys())

    if GEN_SHEET not in book:
        raise RuntimeError(f"Sheet '{GEN_SHEET}' fehlt. Gefunden: {sheets}")
    pipe_sheet = next((s for s in PIPE_SHEET_CANDIDATES if s in book), None)
    if not pipe_sheet:
        raise RuntimeError(f"Sheet für Pipeline fehlt. Erwartet eines von {PIPE_SHEET_CANDIDATES}. Gefunden: {sheets}")

    gen = book[GEN_SHEET].copy()
    pipe = book[pipe_sheet].copy()

    # Checks
    miss_gen = {KEY_COL, PE_COL} - set(gen.columns)
    if miss_gen:
        raise RuntimeError(f"Im Sheet '{GEN_SHEET}' fehlen Spalten: {sorted(miss_gen)}")
    if KEY_COL not in pipe.columns:
        raise RuntimeError(f"Im Sheet '{pipe_sheet}' fehlt Spalte '{KEY_COL}'")

    # fehlende Flags anlegen
    for _, _, flag_col in SCENARIOS:
        if flag_col not in pipe.columns:
            pipe[flag_col] = 1.0
    miss_d = [d for _, d, _ in SCENARIOS if d not in pipe.columns]
    if miss_d:
        raise RuntimeError(f"Im Sheet '{pipe_sheet}' fehlen Distanz-Spalten: {miss_d}")

    # Merge
    work = pipe.merge(gen[[KEY_COL, PE_COL]], on=KEY_COL, how="left", validate="one_to_one")

    # Entscheidungen & Flag-Updates
    for scen, dist_col, flag_col in SCENARIOS:
        d_used = work[dist_col].apply(safe_float) * DIST_FACTOR
        pe = work[PE_COL].apply(safe_float)

        decs, fvals = [], []
        for di, pei in zip(d_used, pe):
            if np.isnan(di) or np.isnan(pei):
                decs.append("nicht bauen")
                fvals.append(np.nan)
            else:
                dec, fval = decision(di, pei)
                dec_norm = str(dec).strip().lower()
                if dec_norm not in ("bauen", "nicht bauen"):
                    raise ValueError(f"decision() gab '{dec}' zurück (erwartet 'bauen' oder 'nicht bauen').")
                decs.append(dec_norm)
                fvals.append(fval)

        before = pd.to_numeric(work[flag_col], errors="coerce").fillna(1.0)
        after = before.mask(pd.Series(decs, index=work.index) == "nicht bauen", 0.0)
        work[flag_col] = after

    # Entfernen: alle drei Flags == 0
    all_zero = (
        (work["Built Scenario 1 (EHB)"].fillna(0).astype(float) == 0)
    )
    to_remove: Set[str] = set(work.loc[all_zero, KEY_COL].astype(str))
    print(f"[i] Anlagen gelöscht / nicht bestanden (beide Szenarien = 'nicht bauen'): {len(to_remove)}")

    # Mappe vorbereiten
    out_book = {}
    for s, df in book.items():
        df2 = df.copy()
        if KEY_COL in df2.columns and to_remove:
            df2 = df2[~df2[KEY_COL].astype(str).isin(to_remove)].copy()
        if s == pipe_sheet:
            save_cols = [c for c in work.columns if c != PE_COL]  # PE nicht doppelt
            df2 = work.loc[:, save_cols].copy()
        out_book[s] = df2
    
    # ---- SYNC: Alle Sheets mit General Data synchronisieren ----
    # Stellt sicher, dass alle Sheets nur Codes enthalten, die in General Data sind
    if GEN_SHEET in out_book and KEY_COL in out_book[GEN_SHEET].columns:
        general_codes = set(out_book[GEN_SHEET][KEY_COL].astype(str).str.strip())
        for s in sheets:
            if s != GEN_SHEET and KEY_COL in out_book[s].columns:
                before = len(out_book[s])
                out_book[s] = out_book[s][out_book[s][KEY_COL].astype(str).str.strip().isin(general_codes)].copy()
                after = len(out_book[s])
                if before != after:
                    print(f"[i] {s}: {before} → {after} Zeilen (sync mit General Data)")

    # In Temp schreiben & atomar ersetzen (mit Retries)
    temp_path = excel_path.with_suffix(excel_path.suffix + ".tmp")
    with pd.ExcelWriter(temp_path, engine="openpyxl") as wr:
        for s in sheets:
            out_book[s].to_excel(wr, sheet_name=s, index=False)

    atomic_replace(temp_path, excel_path)

    # GPKG mit gelöschten Anlagen schreiben
    write_deleted_to_gpkg(root, gen, to_remove)

    print("\n== Update complete ==")
    print(f"Überschrieben: {excel_path}")

if __name__ == "__main__":
    run()