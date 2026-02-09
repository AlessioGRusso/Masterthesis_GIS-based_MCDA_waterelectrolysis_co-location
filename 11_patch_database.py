
from __future__ import annotations
import sys
import re
from pathlib import Path
from typing import Dict, Tuple, Optional

import pandas as pd


# Konfiguration - hier kann man die Pfade anpassen
PROJECT_ROOT = Path(__file__).resolve().parent  # Skript sollte im Projektordner liegen

# Input-Dateien
FW_FILE = PROJECT_ROOT / "Daten" / "DistrictHeating_Database.xlsx"
FW_SHEET: Optional[str] = None
FW_NAME_COL = "Name"
FW_COMMENTS_COL = "Comments"

# Ziel-Datei die gepatcht werden soll
TARGET_FILE = PROJECT_ROOT / "Output"/ "UWWTD_TP_Database.xlsx"
TARGET_NAME_COL = "Name"
TARGET_LAT_COL = "Latitude"
TARGET_LON_COL = "Longitude"

# Output-Einstellungen
OUT_FILE: Optional[Path] = None  # None => "<TARGET_FILE.stem>_updated.xlsx" im gleichen Ordner
CHANGES_CSV: Optional[Path] = PROJECT_ROOT / "Output" / "database_changes_log.csv"

STRICT_NAMES = False  # False => robustes Name-Matching


def normalize_number(s: str) -> Optional[float]:
    """Normalisiert Zahlen aus verschiedenen Formaten"""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    s = s.replace(" ", "")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def normalize_name(n: str) -> str:
    if n is None:
        return ""
    s = str(n).strip().lower()
    s = re.sub(r"\s+", " ", s)
    # Normalisiere Umlaute für besseres Matching
    s = s.replace("ï", "i").replace("ä", "a").replace("ö", "o").replace("ü", "u")
    return s


def parse_latlon_from_comment(text: str) -> Optional[Tuple[float, float]]:
    if not isinstance(text, str) or not text.strip():
        return None
    lower = text.lower()
    if not ("falsch" in lower and "koordinat" in lower):
        return None

    m_phrase = re.search(r"falsch\w*\s+koordinat\w*", lower)
    start_idx = m_phrase.end() if m_phrase else 0
    num_pat = re.compile(r"([+-]?\d+(?:[\.,]\d+)?)")
    nums = num_pat.findall(text[start_idx:])
    if len(nums) >= 2:
        lat = normalize_number(nums[0])
        lon = normalize_number(nums[1])
        if lat is not None and lon is not None:
            return lat, lon

    alt_pats = [
        r"(?:lat(?:itude)?|x)\s*[:=]?\s*([+-]?\d[\d\.\,]*)\D+(?:lon(?:gitude)?|y)\s*[:=]?\s*([+-]?\d[\d\.\,]*)",
        r"(?:lon(?:gitude)?|y)\s*[:=]?\s*([+-]?\d[\d\.\,]*)\D+(?:lat(?:itude)?|x)\s*[:=]?\s*([+-]?\d[\d\.\,]*)",
    ]
    for pat in alt_pats:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            a = normalize_number(m.group(1))
            b = normalize_number(m.group(2))
            if a is not None and b is not None:
                if pat.startswith("(?:lon"):
                    return b, a
                return a, b
    return None


def find_column(df: pd.DataFrame, preferred: str, fallbacks=None) -> Optional[str]:
    if fallbacks is None:
        fallbacks = []
    targets = [preferred] + fallbacks
    lower_map = {c.lower(): c for c in df.columns}
    for t in targets:
        if t.lower() in lower_map:
            return lower_map[t.lower()]
    return None


def read_fw_sheet(path: Path, sheet_name: Optional[str]) -> pd.DataFrame:
    if sheet_name:
        return pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
    return pd.read_excel(path, engine="openpyxl")


def main() -> int:
    fw_path = Path(FW_FILE)
    target_path = Path(TARGET_FILE)
    out_path = target_path  # in-place: überschreibt die Originaldatei
    changes_path = Path(CHANGES_CSV) if CHANGES_CSV else Path(__file__).resolve().parent / "coordinate_changes_log.csv"



    try:
        fw_df = read_fw_sheet(fw_path, FW_SHEET)
    except Exception as e:
        print(f"[FEHLER] Fernwärme-Datei konnte nicht gelesen werden: {e}", file=sys.stderr)
        return 1

    for col in [FW_NAME_COL, FW_COMMENTS_COL]:
        if col not in fw_df.columns:
            print(f"[FEHLER] Spalte '{col}' fehlt in Fernwärme-Datei.", file=sys.stderr)
            return 2

    fw_df = fw_df[[FW_NAME_COL, FW_COMMENTS_COL]].copy()
    fw_df[FW_NAME_COL] = fw_df[FW_NAME_COL].astype(str)

    mapping: Dict[str, Tuple[float, float, str, str]] = {}
    for _, row in fw_df.iterrows():
        name = row[FW_NAME_COL]
        comment = row[FW_COMMENTS_COL]
        parsed = parse_latlon_from_comment(comment)
        if parsed is None:
            continue
        lat, lon = parsed
        key = name if STRICT_NAMES else normalize_name(name)
        mapping[key] = (lat, lon, name, str(comment))

    if not mapping:
        print("[HINWEIS] Keine verwertbaren 'Falsche Koordinaten' gefunden. Abbruch ohne Änderungen.")
        return 0

    try:
        all_sheets = pd.read_excel(target_path, sheet_name=None, engine="openpyxl")
    except Exception as e:
        print(f"[FEHLER] Ziel-DB konnte nicht gelesen werden: {e}", file=sys.stderr)
        return 3

    change_rows = []
    updated_sheets = {}
    total_updates = 0
    total_capacity_updates = 0
    
    # Sammle alle Aire, Pinedo, Palma und Bucuresti Anlagen über alle Sheets
    aire_plants = []  # (sheet_name, df_index, name, capacity, ww)
    pinedo_plants = []  # (sheet_name, df_index, name, capacity, ww)
    palma_plants = []  # (sheet_name, df_index, name, capacity, ww)
    bucuresti_plants = []  # (sheet_name, df_index, name, capacity, ww)

    for sheet_name, df in all_sheets.items():
        name_col = find_column(df, TARGET_NAME_COL)
        lat_col = find_column(df, TARGET_LAT_COL, fallbacks=["Lat", "Latitude (deg)", "LAT"])
        lon_col = find_column(df, TARGET_LON_COL, fallbacks=["Lon", "Long", "Longitude (deg)", "LON"])
        capacity_col = find_column(df, "Capacity/PE", fallbacks=["PE", "Capacity", "PE (p.e.)", "Population Equivalent", "p.e.", "pe"])
        treated_ww_col = find_column(df, "Waste Water Treated [m³/a]", fallbacks=["Treated Wastewater", "Treated WW", "Wastewater Treated", "WW", "Wastewater", "Treated wastewater (m3/year)"])

        if not name_col:
            print(f"[WARN] Sheet '{sheet_name}' ohne Name-Spalte – übersprungen.")
            updated_sheets[sheet_name] = df
            continue

        match_series = df[name_col].astype(str)
        norm_names = match_series if STRICT_NAMES else match_series.apply(normalize_name)

        # Berechne Treated Wastewater aus Capacity wenn nicht vorhanden
        # Literaturwert: 150 L/(Person·Tag) = 54.75 m³/(Person·Jahr)
        if capacity_col and treated_ww_col:
            missing_ww_mask = df[treated_ww_col].isna() & df[capacity_col].notna()
            if missing_ww_mask.any():
                for idx in df.index[missing_ww_mask]:
                    capacity = df.at[idx, capacity_col]
                    calculated_ww = capacity * 54.75
                    df.at[idx, treated_ww_col] = calculated_ww
                    change_rows.append({
                        "sheet": sheet_name,
                        "name": df.at[idx, name_col],
                        "field": "Waste Water Treated",
                        "old_value": "NaN",
                        "new_value": f"{calculated_ww:.2f}",
                        "source": f"Calculated from Capacity ({capacity}) × 54.75 m³/(PE·a)",
                    })
                print(f"[INFO] Sheet '{sheet_name}': {missing_ww_mask.sum()} Wastewater-Werte aus Capacity berechnet")

        # Koordinaten-Updates
        if lat_col and lon_col:
            updates_mask = norm_names.isin(mapping.keys())
            if updates_mask.any():
                for idx in df.index[updates_mask]:
                    key = norm_names.loc[idx]
                    lat_new, lon_new, orig_name, src_comment = mapping[key]
                    lat_old = df.at[idx, lat_col]
                    lon_old = df.at[idx, lon_col]
                    change_rows.append({
                        "sheet": sheet_name,
                        "name": df.at[idx, name_col],
                        "field": "coordinates",
                        "old_value": f"({lat_old}, {lon_old})",
                        "new_value": f"({lat_new}, {lon_new})",
                        "source": src_comment,
                    })

                df.loc[updates_mask, lat_col] = norm_names[updates_mask].map(lambda k: mapping[k][0]).values
                df.loc[updates_mask, lon_col] = norm_names[updates_mask].map(lambda k: mapping[k][1]).values
                total_updates += int(updates_mask.sum())

        # Manchester Salford PE/Capacity Patch
        manchester_mask = norm_names.str.contains("manchester", case=False, na=False) & \
                          norm_names.str.contains("salford", case=False, na=False)
        if manchester_mask.any():
            print(f"[DEBUG] Sheet '{sheet_name}': Gefunden Manchester Salford, capacity_col={capacity_col}")
            if capacity_col:
                for idx in df.index[manchester_mask]:
                    old_capacity = df.at[idx, capacity_col]
                    change_rows.append({
                        "sheet": sheet_name,
                        "name": df.at[idx, name_col],
                        "field": "PE/Capacity",
                        "old_value": str(old_capacity),
                        "new_value": "1200000",
                        "source": "Manual patch for Manchester Salford",
                    })
                df.loc[manchester_mask, capacity_col] = 1200000
                total_capacity_updates += int(manchester_mask.sum())
            else:
                print(f"[WARN] Sheet '{sheet_name}': Manchester Salford gefunden, aber keine Capacity-Spalte!")

        # Aire 2 Capacity Patch
        aire2_mask = norm_names.str.contains(r"\baire\s*2\b", case=False, na=False, regex=True)
        if aire2_mask.any() and capacity_col:
            for idx in df.index[aire2_mask]:
                old_capacity = df.at[idx, capacity_col]
                change_rows.append({
                    "sheet": sheet_name,
                    "name": df.at[idx, name_col],
                    "field": "PE/Capacity",
                    "old_value": str(old_capacity),
                    "new_value": "1000000",
                    "source": "Manual patch for Aire 2",
                })
            df.loc[aire2_mask, capacity_col] = 1000000
            total_capacity_updates += int(aire2_mask.sum())

        # HydroWASTE Namen-Patches für Balkan-Anlagen
        hydrowaste_name_patches = {
            "HW_56333": "Tirana",
            "HW_56475": "Belgrade",
            "HW_56525": "Novi Sad",
            "HW_56424": "Sarajevo",
        }
        
        # Suche nach UWWTD Code Spalte
        code_col = find_column(df, "UWWTD Code", fallbacks=["Code", "uwwCode"])
        
        if code_col and name_col:
            for hw_code, new_name in hydrowaste_name_patches.items():
                hw_mask = df[code_col].astype(str) == hw_code
                if hw_mask.any():
                    for idx in df.index[hw_mask]:
                        old_name = df.at[idx, name_col]
                        if pd.isna(old_name) or str(old_name).strip() == "" or str(old_name).lower() == "nan":
                            df.at[idx, name_col] = new_name
                            change_rows.append({
                                "sheet": sheet_name,
                                "name": hw_code,
                                "field": "Name",
                                "old_value": str(old_name),
                                "new_value": new_name,
                                "source": "HydroWASTE Balkan plant name patch",
                            })
                            total_capacity_updates += 1

        # Sammle Aire-Anlagen
        aire_mask = norm_names.str.contains(r"\baire\b", case=False, na=False, regex=True) & \
                    ~norm_names.str.contains("buenos", case=False, na=False)
        if aire_mask.any():
            print(f"[DEBUG] Sheet '{sheet_name}': Gefunden {aire_mask.sum()} Aire-Anlagen, capacity_col={capacity_col}, ww_col={treated_ww_col}")
        for idx in df.index[aire_mask]:
            capacity = df.at[idx, capacity_col] if capacity_col else 0
            ww = df.at[idx, treated_ww_col] if treated_ww_col else 0
            aire_plants.append((sheet_name, idx, df.at[idx, name_col], capacity, ww))

        # Sammle Pinedo-Anlagen
        pinedo_mask = norm_names.str.contains("pinedo", case=False, na=False)
        if pinedo_mask.any():
            print(f"[DEBUG] Sheet '{sheet_name}': Gefunden {pinedo_mask.sum()} Pinedo-Anlagen, capacity_col={capacity_col}, ww_col={treated_ww_col}")
        for idx in df.index[pinedo_mask]:
            capacity = df.at[idx, capacity_col] if capacity_col else 0
            ww = df.at[idx, treated_ww_col] if treated_ww_col else 0
            pinedo_plants.append((sheet_name, idx, df.at[idx, name_col], capacity, ww))

        # Sammle Palma-Anlagen
        palma_mask = norm_names.str.contains("palma", case=False, na=False)
        if palma_mask.any():
            print(f"[DEBUG] Sheet '{sheet_name}': Gefunden {palma_mask.sum()} Palma-Anlagen, capacity_col={capacity_col}, ww_col={treated_ww_col}")
        for idx in df.index[palma_mask]:
            capacity = df.at[idx, capacity_col] if capacity_col else 0
            ww = df.at[idx, treated_ww_col] if treated_ww_col else 0
            palma_plants.append((sheet_name, idx, df.at[idx, name_col], capacity, ww))

        # Sammle Bucuresti-Anlagen
        bucuresti_mask = norm_names.str.contains("bucuresti", case=False, na=False)
        if bucuresti_mask.any():
            print(f"[DEBUG] Sheet '{sheet_name}': Gefunden {bucuresti_mask.sum()} Bucuresti-Anlagen, capacity_col={capacity_col}, ww_col={treated_ww_col}")
        for idx in df.index[bucuresti_mask]:
            capacity = df.at[idx, capacity_col] if capacity_col else 0
            ww = df.at[idx, treated_ww_col] if treated_ww_col else 0
            bucuresti_plants.append((sheet_name, idx, df.at[idx, name_col], capacity, ww))

        updated_sheets[sheet_name] = df

    # Merge Aire-Anlagen über alle Sheets
    if len(aire_plants) >= 2:
        primary_sheet, primary_idx, primary_name, _, _ = aire_plants[0]
        total_capacity = sum(p[3] for p in aire_plants)
        total_ww = sum(p[4] for p in aire_plants)
        

        df = updated_sheets[primary_sheet]
        name_col = find_column(df, TARGET_NAME_COL)
        capacity_col = find_column(df, "Capacity/PE", fallbacks=["PE", "Capacity", "PE (p.e.)", "Population Equivalent", "p.e.", "pe"])
        treated_ww_col = find_column(df, "Waste Water Treated [m³/a]", fallbacks=["Treated Wastewater", "Treated WW", "Wastewater Treated", "WW", "Wastewater", "Treated wastewater (m3/year)"])
        
        if capacity_col:
            df.at[primary_idx, capacity_col] = total_capacity
        if treated_ww_col:
            df.at[primary_idx, treated_ww_col] = total_ww
        

        merged_names = ", ".join(f"{p[2]} ({p[0]})" for p in aire_plants)
        change_rows.append({
            "sheet": "ALL",
            "name": primary_name,
            "field": "merge",
            "old_value": merged_names,
            "new_value": f"Capacity: {total_capacity}, WW: {total_ww}",
            "source": "Aire plants merged across all sheets",
        })
        
        # Entferne alle anderen Aire-Einträge
        for sheet_name, idx, _, _, _ in aire_plants[1:]:
            df = updated_sheets[sheet_name]
            df.drop(idx, inplace=True)

    # Merge Pinedo-Anlagen über alle Sheets
    if len(pinedo_plants) >= 2:
        primary_sheet, primary_idx, primary_name, _, _ = pinedo_plants[0]
        total_capacity = sum(p[3] for p in pinedo_plants)
        total_ww = sum(p[4] for p in pinedo_plants)
        
        # Update primary plant
        df = updated_sheets[primary_sheet]
        name_col = find_column(df, TARGET_NAME_COL)
        capacity_col = find_column(df, "Capacity/PE", fallbacks=["PE", "Capacity", "PE (p.e.)", "Population Equivalent", "p.e.", "pe"])
        treated_ww_col = find_column(df, "Waste Water Treated [m³/a]", fallbacks=["Treated Wastewater", "Treated WW", "Wastewater Treated", "WW", "Wastewater", "Treated wastewater (m3/year)"])
        
        if capacity_col:
            df.at[primary_idx, capacity_col] = total_capacity
        if treated_ww_col:
            df.at[primary_idx, treated_ww_col] = total_ww
        

        merged_names = ", ".join(f"{p[2]} ({p[0]})" for p in pinedo_plants)
        change_rows.append({
            "sheet": "ALL",
            "name": primary_name,
            "field": "merge",
            "old_value": merged_names,
            "new_value": f"Capacity: {total_capacity}, WW: {total_ww}",
            "source": "Pinedo plants merged across all sheets",
        })
        
        # Entferne alle anderen Pinedo-Einträge
        for sheet_name, idx, _, _, _ in pinedo_plants[1:]:
            df = updated_sheets[sheet_name]
            df.drop(idx, inplace=True)

    # Merge Palma-Anlagen über alle Sheets
    if len(palma_plants) >= 2:
        primary_sheet, primary_idx, primary_name, _, _ = palma_plants[0]
        total_capacity = sum(p[3] for p in palma_plants)
        total_ww = sum(p[4] for p in palma_plants)
        

        df = updated_sheets[primary_sheet]
        name_col = find_column(df, TARGET_NAME_COL)
        capacity_col = find_column(df, "Capacity/PE", fallbacks=["PE", "Capacity", "PE (p.e.)", "Population Equivalent", "p.e.", "pe"])
        treated_ww_col = find_column(df, "Waste Water Treated [m³/a]", fallbacks=["Treated Wastewater", "Treated WW", "Wastewater Treated", "WW", "Wastewater", "Treated wastewater (m3/year)"])
        
        if capacity_col:
            df.at[primary_idx, capacity_col] = total_capacity
        if treated_ww_col:
            df.at[primary_idx, treated_ww_col] = total_ww
        

        merged_names = ", ".join(f"{p[2]} ({p[0]})" for p in palma_plants)
        change_rows.append({
            "sheet": "ALL",
            "name": primary_name,
            "field": "merge",
            "old_value": merged_names,
            "new_value": f"Capacity: {total_capacity}, WW: {total_ww}",
            "source": "Palma plants merged across all sheets",
        })
        
        # Entferne alle anderen Palma-Einträge
        for sheet_name, idx, _, _, _ in palma_plants[1:]:
            df = updated_sheets[sheet_name]
            df.drop(idx, inplace=True)

    # Merge Bucuresti-Anlagen über alle Sheets
    if len(bucuresti_plants) >= 2:
        primary_sheet, primary_idx, primary_name, _, _ = bucuresti_plants[0]
        total_capacity = sum(p[3] for p in bucuresti_plants)
        total_ww = sum(p[4] for p in bucuresti_plants)
        

        df = updated_sheets[primary_sheet]
        name_col = find_column(df, TARGET_NAME_COL)
        capacity_col = find_column(df, "Capacity/PE", fallbacks=["PE", "Capacity", "PE (p.e.)", "Population Equivalent", "p.e.", "pe"])
        treated_ww_col = find_column(df, "Waste Water Treated [m³/a]", fallbacks=["Treated Wastewater", "Treated WW", "Wastewater Treated", "WW", "Wastewater", "Treated wastewater (m3/year)"])
        
        if capacity_col:
            df.at[primary_idx, capacity_col] = total_capacity
        if treated_ww_col:
            df.at[primary_idx, treated_ww_col] = total_ww
        

        merged_names = ", ".join(f"{p[2]} ({p[0]})" for p in bucuresti_plants)
        change_rows.append({
            "sheet": "ALL",
            "name": primary_name,
            "field": "merge",
            "old_value": merged_names,
            "new_value": f"Capacity: {total_capacity}, WW: {total_ww}",
            "source": "Bucuresti plants merged across all sheets",
        })
        
        # Entferne alle anderen Bucuresti-Einträge
        for sheet_name, idx, _, _, _ in bucuresti_plants[1:]:
            df = updated_sheets[sheet_name]
            df.drop(idx, inplace=True)

    total_merges = (1 if len(aire_plants) >= 2 else 0) + (1 if len(pinedo_plants) >= 2 else 0) + (1 if len(palma_plants) >= 2 else 0) + (1 if len(bucuresti_plants) >= 2 else 0)

    if total_updates == 0 and total_capacity_updates == 0 and total_merges == 0:
        print("[HINWEIS] Keine Änderungen vorgenommen.")
        return 0

    log_df = pd.DataFrame(change_rows, columns=["sheet", "name", "field", "old_value", "new_value", "source"])
    try:
        log_df.to_csv(changes_path, index=False, encoding="utf-8")
        print(f"✓ Change log saved → {changes_path.name}")
    except Exception as e:
        print(f"[WARN] Änderungen-Log konnte nicht geschrieben werden: {e}", file=sys.stderr)

    try:
        with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as writer:
            for sn, sdf in updated_sheets.items():
                sdf.to_excel(writer, sheet_name=sn, index=False)
        print(f"✓ {total_updates} coordinates updated, {total_capacity_updates} capacities updated, {total_merges} plants merged → {out_path.name}")
    except Exception as e:
        print(f"[FEHLER] Ausgabedatei konnte nicht geschrieben werden: {e}", file=sys.stderr)
        return 4

    return 0


if __name__ == "__main__":
    pd.options.display.width = 200
    sys.exit(main())
