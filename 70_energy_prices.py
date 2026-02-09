
import os, sys, time, datetime as dt, re
from typing import Dict, Tuple, Optional, List
import numpy as np
import pandas as pd
import requests
from lxml import etree
from dateutil import tz

# Konfiguration
API_TOKEN = "ab6b670f-8200-4c3f-aea0-197c7a8713bf"

# Excel
EXCEL_PATH = r"Output/UWWTD_TP_Database.xlsx"
SHEET_GENERAL = "General Data"
SHEET_ENERGY  = "Grid Energy Connection"

# Spaltennamen
LAT_COL = "Latitude"
LON_COL = "Longitude"
ID_COL_CANDIDATES = ["ID", "PlantID", "WWTP_ID", "Name"]
EIC_OVERRIDE_COL_CANDIDATES = ["EIC", "BiddingZoneEIC", "ZoneEIC"]

# Zeitraum
_today = dt.date.today()
YEAR = 2024
DATE_FROM = dt.date(YEAR, 1, 1)
DATE_TO   = dt.date(YEAR, 12, 31)
TZ_LOCAL = "Europe/Berlin"

# I/O
CSV_DELIMITER = ';'
OUT_ZONES_DIR = os.path.join("Output", "Energy price datasets", "zones")
VERBOSE = True
PAUSE_BETWEEN_CALLS = 0.35
pd.options.mode.copy_on_write = True

# Cache
CACHE_USE_EXISTING = True
FORCE_REFRESH_EICS: set[str] = set()

# Eurostat
SURCHARGES_CSV = os.path.join("Input", "eurostat_surcharges.csv")

# Deutschland CSV-Fallback
DE_PRICES_CSV = os.path.join("Daten", "Energy Prices", "Gro_handelspreise_202312310000_202501010000_Stunde.csv")

# Eurostat-Band IE
EUROSTAT_BAND = "IE"   # vorher IG

# Standard-VAT + Fallback
SURCHARGES_FALLBACK: Dict[str, Dict[str, float]] = {
    "DEFAULT": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 0.0},
    "DE": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 19.0},
    "LU": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 17.0},
    "FR": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 20.0},
    "IT": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 22.0},
    "ES": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 21.0},
    "NL": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 21.0},
    "BE": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 21.0},
    "AT": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 20.0},
    "PL": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 23.0},
    "PT": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 23.0},
    "IE": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 23.0},
    "HU": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 27.0},
    "SK": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 20.0},
    "SI": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 22.0},
    "HR": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 25.0},
    "RO": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 19.0},
    "BG": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 20.0},
    "GR": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 24.0},
    "CZ": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 21.0},
    "LT": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 21.0},
    "LV": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 21.0},
    "EE": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 22.0},
    "FI": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 24.0},
    "DK": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 25.0},
    "SE": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 25.0},
    "NO": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 25.0},
    "CH": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 8.1},
    "TR": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 20.0},
    "GB": {"grid_fee_ct": 0.0, "levies_ct": 0.0, "tax_ct": 0.0, "vat_pct": 20.0},
}

# -------------------- GB (Great Britain) — fester Preis & Levies --------------------
USE_GB_FIXED_MEAN = True         # GB nicht aus ENTSO-E/CSV ziehen, sondern Festwert verwenden
GBP_EUR = 1.181                  # gleicher Kurs
GB_WHOLESALE_GBP_PER_MWH = 98.0
GB_DUOS_HV_CT = 0.575            # ct/kWh (MV/HV, R/A/G ~ 10/40/50), konservativer GB-Mittelwert
GB_BSUOS_GBP_PER_MWH = 12.48     # £/MWh
GB_CCL_GBP_PER_KWH   = 0.00775   # £/kWh
GB_VAT_PCT           = 20.0      # %

# Abgeleitete ct/kWh
GB_SPOT_CT   = GB_WHOLESALE_GBP_PER_MWH * GBP_EUR / 10.0
GB_BSUOS_CT  = GB_BSUOS_GBP_PER_MWH   * GBP_EUR / 10.0
GB_CCL_CT    = GB_CCL_GBP_PER_KWH     * GBP_EUR * 100.0

# ENTSO-E
ENTSOE_API = "https://web-api.tp.entsoe.eu/api"
HEADERS = {"User-Agent": "entsoe-zone-annual/2.11 (contact: you@example.com)"}

# Bidding-Zone EICs (geprüft)
SINGLE_ZONE_EIC: Dict[str, str] = {
    "DE": "10Y1001A1001A83F",  # DE-LU
    "AT": "10YAT-APG------L",
    "NL": "10YNL----------L",
    "BE": "10YBE----------2",
    "FR": "10YFR-RTE------C",
    "CH": "10YCH-SWISSGRIDZ",
    "CZ": "10YCZ-CEPS-----N",
    "PL": "10YPL-AREA-----S",
    "PT": "10YPT-REN------W",
    "IE": "10YIE-1001A00010",
    "HU": "10YHU-MAVIR----U",
    "SK": "10YSK-SEPS-----K",
    "SI": "10YSI-ELES-----O",
    "HR": "10YHR-HEP------M",
    "RO": "10YRO-TEL------P",
    "BG": "10YCA-BULGARIA-R",
    "GR": "10YGR-HTSO-----Y",
    "TR": "10YTR-TEIAS----W",
    "LT": "10YLT-1001A0008Q",
    "LV": "10YLV-1001A00074",
    "EE": "10Y1001A1001A39I",
    "FI": "10YFI-1--------U",
    "GB": "10YGB----------A",
}
EIC_BY_ZONE = {
    "DK1": "10YDK-1--------W", "DK2": "10YDK-2--------M",
    "SE1": "10Y1001A1001A44P", "SE2": "10Y1001A1001A45N",
    "SE3": "10Y1001A1001A46L", "SE4": "10Y1001A1001A47J",
    "NO1": "10YNO-1--------2", "NO2": "10YNO-2--------T",
    "NO3": "10YNO-3--------J", "NO4": "10YNO-4--------9", "NO5": "10YNO-5--------7",
    "IT-NORD": "10Y1001A1001A73I", "IT-CNOR": "10Y1001A1001A70O",
    "IT-CSUD": "10Y1001A1001A71M", "IT-SUD":  "10Y1001A1001A788",
    "IT-SICI": "10Y1001A1001A75E", "IT-SARD": "10Y1001A1001A74G",
    "ES-MAIN": "10YES-REE------0",
}
KNOWN_NO_A44 = {"10YGB----------A"}  # GB via API oft unzuverlässig → bei uns ohnehin Festwert

# Falls alte Area-Codes reinrutschen, automatisch umbiegen
EIC_FALLBACK = {
    "10YEE-1001A00028": "10Y1001A1001A39I",  # EE Area → EE BZN
}

# ===================== Utils =====================
def pbar(iterable, desc=""):
    try:
        from tqdm import tqdm
        return tqdm(iterable, desc=desc, unit="it")
    except Exception:
        if VERBOSE: print(f"[i] tqdm nicht installiert – {desc}")
        return iterable

def pick_first_existing_column(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns: return c
        for col in df.columns:
            if col.lower() == c.lower(): return col
    return None

def _fmt_utc(dtobj: dt.datetime) -> str:
    return dtobj.strftime("%Y%m%d%H%M")

# ===================== Excel-Preflight =====================
def assert_excel_readable(path: str, sheet_general: str):
    if not os.path.exists(path):
        raise SystemExit(f"Excel-Datei nicht gefunden:\n  {os.path.abspath(path)}")
    try:
        with open(path, "rb") as f:
            sig = f.read(4)
        if sig != b"PK\x03\x04":
            raise SystemExit("Die Datei ist keine gültige .xlsx (Zip). Bitte in Excel als XLSX neu speichern.")
    except PermissionError:
        raise SystemExit("Kein Zugriff auf die Datei (gesperrt?). Schließe Excel & versuche erneut.")
    try:
        xls = pd.ExcelFile(path, engine="openpyxl")
        if sheet_general not in xls.sheet_names:
            raise SystemExit(f"Sheet '{sheet_general}' nicht gefunden. Vorhanden: {xls.sheet_names}")
    except Exception as ex:
        raise SystemExit(f"Kann Excel nicht öffnen ({type(ex).__name__}): {ex}")

# ===================== Eurostat Auto-Fetch (für alle außer GB) =====================
EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data"
_EUROSTAT_GEO_FIX = {"EL":"GR", "UK":"GB"}  # wichtigste Abweichungen

# Zonen mit fehlerhaften Caches
FAILED_EICS_FILE = os.path.join(OUT_ZONES_DIR, "failed_eics.txt")

def load_failed_eics() -> set:
    """Lädt Liste der EICs, die beim letzten Lauf fehlgeschlagen sind."""
    if not os.path.exists(FAILED_EICS_FILE):
        return set()
    try:
        with open(FAILED_EICS_FILE, 'r') as f:
            return {line.strip() for line in f if line.strip()}
    except Exception:
        return set()

def save_failed_eics(failed: set):
    """Speichert Liste der fehlgeschlagenen EICs."""
    try:
        with open(FAILED_EICS_FILE, 'w') as f:
            for eic in sorted(failed):
                f.write(f"{eic}\n")
    except Exception:
        pass

def fetch_eurostat_annual_price(iso2: str, year: int, band_code: str = EUROSTAT_BAND) -> Optional[float]:
    """
    Holt den Eurostat-Jahresmittelpreis (I_TAX: ohne MwSt, aber inkl. Netzentgelte/Abgaben) für ein Land.
    Gibt ct/kWh zurück oder None bei Fehler.
    Dataset: nrg_pc_204 (Electricity prices for non-household consumers)
    """
    import io
    try:
        url = f"{EUROSTAT_BASE}/nrg_pc_204"
        r = requests.get(url, params={"format": "TSV", "compressed": "false"}, timeout=120, headers=HEADERS)
        r.raise_for_status()

        raw = pd.read_csv(io.StringIO(r.text), sep="\t", comment="#", dtype=str, keep_default_na=False)
        raw.columns = [c.strip() for c in raw.columns]
        key_col = raw.columns[0]
        year_cols = [c.strip() for c in raw.columns[1:] if c.strip().isdigit()]
        
        if not year_cols:
            return None

        long = raw.melt(id_vars=[key_col], value_vars=year_cols, var_name="TIME_PERIOD", value_name="value")
        long["value"] = pd.to_numeric(long["value"].replace({":": None, "": None}), errors="coerce")
        
        dims_header = key_col.replace("\\TIME_PERIOD", "")
        dim_names = [d.strip().lower() for d in dims_header.split(",")]
        dims = long[key_col].str.split(",", n=len(dim_names)-1, expand=True)
        
        if dims.shape[1] != len(dim_names):
            return None
            
        dims.columns = dim_names
        long = pd.concat([dims, long[["TIME_PERIOD", "value"]]], axis=1)

        # Spalten umbenennen
        if "nrg_prc" in long.columns: long = long.rename(columns={"nrg_prc": "tax"})
        if "nrg_cons" in long.columns: long = long.rename(columns={"nrg_cons": "band"})
        
        # Filter
        if "freq" in long.columns: long = long[long["freq"].str.upper().eq("A")]
        if "currency" in long.columns: long = long[long["currency"].str.upper().eq("EUR")]
        
        # Jahr
        long["year"] = pd.to_numeric(long["TIME_PERIOD"].str.extract(r"(\d{4})")[0], errors="coerce")
        avail = long.dropna(subset=["value"]).groupby("year")["value"].size().sort_index()
        if avail.empty:
            return None
        use_year = max([y for y in avail.index if y <= year] or [avail.index.max()])
        long = long[long["year"].eq(use_year)]

        # Band-Filter
        if "band" in long.columns:
            for _code in [band_code, "IF", "ID"]:
                bm = long["band"].astype(str).str.upper().str.strip().eq(str(_code).upper().strip())
                if bm.any():
                    long = long[bm]
                    break

        # ISO2 mapping
        long["ISO2"] = long["geo"].str.upper().map(_EUROSTAT_GEO_FIX).fillna(long["geo"].str.upper())
        
        # Nur "Excluding VAT"
        if "tax" in long.columns:
            long = long[long["tax"].astype(str).str.upper().eq("I_TAX")]
        
        # Preis für das Land
        country_data = long[long["ISO2"].eq(iso2.upper())]
        if country_data.empty:
            return None
            
        price_eur_kwh = country_data["value"].mean()
        if pd.isna(price_eur_kwh):
            return None
            
        return float(price_eur_kwh * 100.0)  # ct/kWh
        
    except Exception as ex:
        if VERBOSE:
            print(f"[WARN] Eurostat-Jahresmittel für {iso2} nicht verfügbar: {ex}")
        return None

def fetch_eurostat_components(year: int, band_code: str = EUROSTAT_BAND) -> Dict[str, Dict[str, float]]:
    """
    Holt nrg_pc_205_c als TSV (compact → long) und liefert je ISO2:
      - grid_fee_ct  ← NETC (Netz)
      - levies_ct    ← TAX_FEE_LEV_CHRG gesamt (Fallback: Summe TAX_* Unterkategorien)
      - tax_ct       ← 0.0 (hier nicht separat)
      - vat_pct      ← aus SURCHARGES_FALLBACK
    Wählt automatisch das neueste verfügbare Jahr ≤ 'year'.
    Band-Filter: bevorzugt 'band_code' (IG), sonst Fallback IF → ID.
    """
    import io
    url = f"{EUROSTAT_BASE}/nrg_pc_205_c"
    r = requests.get(url, params={"format": "TSV", "compressed": "false"}, timeout=120, headers=HEADERS)
    r.raise_for_status()

    raw = pd.read_csv(io.StringIO(r.text), sep="\t", comment="#", dtype=str, keep_default_na=False)
    raw.columns = [c.strip() for c in raw.columns]
    key_col = raw.columns[0]
    year_cols = [c.strip() for c in raw.columns[1:] if c.strip().isdigit()]
    if not year_cols:
        raise ValueError("Eurostat TSV: keine Jahresspalten gefunden.")

    long = raw.melt(id_vars=[key_col], value_vars=year_cols, var_name="TIME_PERIOD", value_name="value")
    long["value"] = pd.to_numeric(long["value"].replace({":": None, "": None}), errors="coerce")
    dims_header = key_col.replace("\\TIME_PERIOD", "")
    dim_names = [d.strip().lower() for d in dims_header.split(",")]
    dims = long[key_col].str.split(",", n=len(dim_names)-1, expand=True)
    if dims.shape[1] != len(dim_names):
        raise ValueError(f"Eurostat TSV-Key unerwartet: {dims_header}")
    dims.columns = dim_names
    long = pd.concat([dims, long[["TIME_PERIOD", "value"]]], axis=1)

    if "nrg_prc" in long.columns: long = long.rename(columns={"nrg_prc": "tax"})
    if "nrg_cons" in long.columns: long = long.rename(columns={"nrg_cons": "band"})
    if "freq" in long.columns:     long = long[long["freq"].str.upper().eq("A")]
    if "currency" in long.columns: long = long[long["currency"].str.upper().eq("EUR")]

    long["year"] = pd.to_numeric(long["TIME_PERIOD"].str.extract(r"(\d{4})")[0], errors="coerce")
    avail = long.dropna(subset=["value"]).groupby("year")["value"].size().sort_index()
    use_year = max([y for y in avail.index if y <= year] or [avail.index.max()])
    long = long[long["year"].eq(use_year)]

    # --- Band-Filter: bevorzugt 'band_code', sonst IF → ID ---
    def _band_mask(series: pd.Series, code: str) -> pd.Series:
        s = series.astype(str).str.upper().str.strip()
        return s.eq(str(code).upper().strip())

    if "band" in long.columns:
        picked = None
        for _code in [band_code, "IF", "ID"]:
            bm = _band_mask(long["band"], _code)
            if bm.any():
                if VERBOSE and _code != band_code:
                    print(f"[i] Eurostat: Band '{band_code}' nicht verfügbar → nutze '{_code}'.")
                long = long[bm]
                picked = _code
                break
        if picked is None and VERBOSE:
            print(f"[WARN] Eurostat: kein passendes Band für Filter {band_code}/IF/ID gefunden — nutze ungefiltert.")

    long["ISO2"] = long["geo"].str.upper().map(_EUROSTAT_GEO_FIX).fillna(long["geo"].str.upper())

    tax_u = long["tax"].astype(str).str.upper()
    is_grid = tax_u.eq("NETC") | tax_u.str.startswith("NET")
    is_lev_total = tax_u.eq("TAX_FEE_LEV_CHRG") | tax_u.eq("NRT")
    LEV_PARTS = {"TAX_ENV","TAX_RNW","TAX_CAP","TAX_NUC","TAX_CAP_ALLOW","TAX_NUC_ALLOW","ALLOW_OTH","OTH"}
    is_lev_parts = tax_u.isin(LEV_PARTS)

    # mean() statt sum()
    # Durchschnitt nehmen
    grid = long.loc[is_grid].groupby("ISO2")["value"].mean().mul(100.0)
    if not long.loc[is_lev_total].empty:
        lev = long.loc[is_lev_total].groupby("ISO2")["value"].mean().mul(100.0)
    elif not long.loc[is_lev_parts].empty:
        # Bei Einzelkomponenten summieren
        lev = long.loc[is_lev_parts].groupby("ISO2")["value"].sum().mul(100.0)
    else:
        lev = pd.Series(dtype=float)

    sur: Dict[str, Dict[str, float]] = {}
    geos = set(grid.index) | set(lev.index)
    for g in geos:
        sur[g] = {
            "grid_fee_ct": float(grid.get(g, np.nan)) if g in grid.index else 0.0,
            "levies_ct":   float(lev.get(g,  np.nan)) if g in lev.index  else 0.0,
            "tax_ct":      0.0,
            "vat_pct":     SURCHARGES_FALLBACK.get(g, SURCHARGES_FALLBACK["DEFAULT"])["vat_pct"],
        }
    sur["DEFAULT"] = SURCHARGES_FALLBACK.get("DEFAULT", {"grid_fee_ct":0.0,"levies_ct":0.0,"tax_ct":0.0,"vat_pct":0.0})
    return sur

def load_surcharges() -> Dict[str, Dict[str, float]]:
    sur: Dict[str, Dict[str, float]] = {}
    try:
        # Nutzt EUROSTAT_BAND
        sur = fetch_eurostat_components(YEAR, band_code=EUROSTAT_BAND)
        if VERBOSE and sur:
            print(f"[i] Eurostat-Komponenten geladen ({YEAR}, Band={EUROSTAT_BAND}): {len(sur)-1} Länder.")
    except Exception as ex:
        if VERBOSE: print(f"[WARN] Eurostat-Komponenten nicht ladbar: {ex}")

    # CSV-Override
    if os.path.exists(SURCHARGES_CSV):
        try:
            df = pd.read_csv(SURCHARGES_CSV)
            need = {"ISO2","grid_fee_ct","levies_ct","tax_ct","vat_pct"}
            if not need.issubset(set(df.columns)):
                raise SystemExit(f"Surcharges CSV muss Spalten {sorted(need)} enthalten.")
            for _,r in df.iterrows():
                iso = str(r["ISO2"]).upper().strip()[:2]
                sur[iso] = {
                    "grid_fee_ct": float(r["grid_fee_ct"]),
                    "levies_ct":   float(r["levies_ct"]),
                    "tax_ct":      float(r["tax_ct"]),
                    "vat_pct":     float(r["vat_pct"]),
                }
            if VERBOSE: print(f"[i] CSV-Override geladen ({len(df)} Einträge).")
        except Exception as ex:
            print(f"[WARN] Konnte CSV-Override nicht laden: {ex}")

    # GB Festwerte in die Surcharges einsetzen
    if USE_GB_FIXED_MEAN:
        sur["GB"] = {
            "grid_fee_ct": GB_DUOS_HV_CT,
            "levies_ct":   GB_BSUOS_CT + GB_CCL_CT,
            "tax_ct":      0.0,
            "vat_pct":     GB_VAT_PCT,
        }

    # Fallback ergänzen/auffüllen
    for iso, cfg in SURCHARGES_FALLBACK.items():
        if iso not in sur:
            sur[iso] = cfg.copy()
        else:
            if sur[iso].get("vat_pct") in (None, 0.0):
                sur[iso]["vat_pct"] = cfg.get("vat_pct", 0.0)

    if "DEFAULT" not in sur:
        sur["DEFAULT"] = SURCHARGES_FALLBACK["DEFAULT"].copy()
    return sur

# ===================== Deutschland CSV Loader =====================
def load_de_prices_from_csv(csv_path: str, year: int) -> pd.DataFrame:
    """Lädt deutsche Energiepreise aus SMARD CSV für ein bestimmtes Jahr."""
    if not os.path.exists(csv_path):
        if VERBOSE: print(f"[WARN] DE CSV nicht gefunden: {csv_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(csv_path, sep=';', decimal=',')
        # Spalte: "Deutschland/Luxemburg [€/MWh] Berechnete Auflösungen"
        de_col = [c for c in df.columns if 'Deutschland' in c and '€/MWh' in c]
        if not de_col:
            if VERBOSE: print(f"[WARN] DE Spalte nicht in CSV gefunden")
            return pd.DataFrame()
        
        de_col = de_col[0]
        df['timestamp_utc'] = pd.to_datetime(df['Datum von'], format='%d.%m.%Y %H:%M', utc=True)
        df['ct_kWh'] = pd.to_numeric(df[de_col], errors='coerce') / 10.0  # €/MWh → ct/kWh
        df['minutes'] = 60  # stündliche Daten
        
        # Filter auf Jahr
        df = df[df['timestamp_utc'].dt.year == year]
        
        result = df[['timestamp_utc', 'ct_kWh', 'minutes']].dropna()
        if VERBOSE and not result.empty:
            print(f"[i] DE CSV geladen: {len(result)} Stunden für {year}")
        return result
    except Exception as ex:
        if VERBOSE: print(f"[WARN] Fehler beim Laden der DE CSV: {ex}")
        return pd.DataFrame()

# ===================== ENTSO-E helpers =====================
def _parse_prices_xml(xml_bytes: bytes, day_local: dt.date, tz_local: str) -> pd.DataFrame:
    root = etree.fromstring(xml_bytes)
    if root.tag.endswith("Acknowledgement_MarketDocument"):
        return pd.DataFrame()
    ts_list = root.xpath("//*[local-name()='TimeSeries']")
    rows = []
    local_tz = tz.gettz(tz_local)

    def one_text(node, xp: str) -> Optional[str]:
        try:
            res = node.xpath(xp); return res[0] if res else None
        except etree.XPathEvalError:
            return None

    for ts in ts_list:
        for period in ts.xpath(".//*[local-name()='Period']"):
            start_txt = one_text(period, ".//*[local-name()='timeInterval']/*[local-name']='start']/text()")
            if not start_txt:
                start_txt = one_text(period, ".//*[local-name()='start']/text()")
            res_txt = one_text(period, ".//*[local-name()='resolution']/text()")
            if not (start_txt and res_txt): continue
            try:
                m = re.match(r"PT(\d+)M", res_txt.strip()); step_min = int(m.group(1)) if m else 60
                start = dt.datetime.fromisoformat(start_txt.replace("Z","+00:00"))
            except Exception:
                continue
            for p in period.xpath("./*[local-name()='Point']"):
                pos_txt = one_text(p, "./*[local-name()='position']/text()")
                val_txt = one_text(p, "./*[local-name()='price.amount']/text()")
                if not (pos_txt and val_txt): continue
                try:
                    pos = int(pos_txt); val = float(val_txt)
                except Exception:
                    continue
                ts_utc = start + dt.timedelta(minutes=step_min*(pos-1))
                ts_local_dt = ts_utc.astimezone(local_tz)
                if ts_local_dt.date() == day_local:
                    rows.append({
                        "timestamp_utc": ts_utc.replace(tzinfo=tz.UTC),
                        "ct_kWh": val/10.0,   # EUR/MWh → ct/kWh
                        "minutes": step_min
                    })
    return pd.DataFrame(rows)

def entsoe_day(eic: str, day_local: dt.date, tz_local: str, max_retries=3) -> Tuple[pd.DataFrame, bool]:
    """
    Holt ENTSO-E Daten für einen Tag.
    Returns: (DataFrame, has_error)
      - has_error=True bedeutet struktureller Fehler (keine Daten verfügbar)
      - has_error=False bedeutet OK (auch wenn DataFrame leer ist wegen Feiertag etc.)
    """
    tzinfo = tz.gettz(tz_local)
    start_local = dt.datetime.combine(day_local, dt.time(0,0,tzinfo=tzinfo))
    end_local   = start_local + dt.timedelta(days=1)
    start_utc = (start_local - dt.timedelta(hours=3)).astimezone(tz.UTC)
    end_utc   = (end_local   + dt.timedelta(hours=3)).astimezone(tz.UTC)
    params = {
        "documentType":"A44","processType":"A01","in_Domain":eic,"out_Domain":eic,
        "periodStart": _fmt_utc(start_utc), "periodEnd": _fmt_utc(end_utc),
        "securityToken": API_TOKEN,
    }
    backoff=2
    for _ in range(max_retries):
        r = requests.get(ENTSOE_API, params=params, headers=HEADERS, timeout=60)
        if r.status_code==429:
            ra=r.headers.get("Retry-After"); wait=int(ra) if (ra and ra.isdigit()) else backoff
            if VERBOSE: print(f"[!] 429 {eic} {day_local}: warte {wait}s")
            time.sleep(wait); backoff=min(backoff*2,60); continue
        try:
            r.raise_for_status()
        except requests.HTTPError as ex:
            if VERBOSE: print(f"[WARN] {eic} {day_local}: HTTP {r.status_code} – {ex}")
            return pd.DataFrame(), True  # Fehler

        content = r.content
        try:
            root = etree.fromstring(content)
            if root.tag.endswith("Acknowledgement_MarketDocument"):
                reason = root.xpath("string(//*[local-name()='Reason']/*[local-name()='text'])")
                code   = root.xpath("string(//*[local-name()='Reason']/*[local-name()='code'])")
                if VERBOSE:
                    print(f"[warn] {eic} {day_local}: ENTSO-E Ack {code or '?'} – {reason or 'kein Grund angegeben'}")
                return pd.DataFrame(), True
        except Exception:
            pass

        return _parse_prices_xml(content, day_local, tz_local), False  # OK
    return pd.DataFrame(), True

def entsoe_range(eic: str, start_date: dt.date, end_date: dt.date, tz_local: str) -> pd.DataFrame:
    parts=[]
    days=(end_date - start_date).days + 1
    for i in pbar(range(days), desc=f"ENTSO-E {eic}"):
        d = start_date + dt.timedelta(days=i)
        df, has_error = entsoe_day(eic, d, tz_local)
        

        if has_error:
            if VERBOSE: print(f"[!] {eic}: Fehler bei {d} → Abbruch, nutze Eurostat-Fallback")
            return pd.DataFrame()
        
        if not df.empty: parts.append(df)
        time.sleep(PAUSE_BETWEEN_CALLS)
    if not parts: return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    out.sort_values("timestamp_utc", inplace=True)
    return out.reset_index(drop=True)

# ===================== Country / Zones =====================
def reverse_geocode_country(lat: float, lon: float) -> Optional[str]:
    try:
        import reverse_geocoder as rg
        res = rg.search([(lat, lon)], mode=1)
        return res[0].get("cc").upper()
    except Exception:
        pass
    url="https://nominatim.openstreetmap.org/reverse"
    params={"format":"json","lat":lat,"lon":lon,"zoom":3,"addressdetails":1}
    r=requests.get(url, params=params, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=20)
    if r.ok:
        data=r.json(); cc=(data.get("address") or {}).get("country_code","")
        return cc.upper() if cc else None
    return None

def zone_dk(lat,lon):
    if 14.6<=lon<=15.3 and 54.9<=lat<=55.4: return "DK2"
    return "DK2" if lon>=11.0 else "DK1"
def zone_se(lat,lon):
    if lat>=63.3: return "SE1"
    if lat>=60.0: return "SE2"
    if lat>=57.0: return "SE3"
    return "SE4"
def zone_no(lat,lon):
    if lat>=68.0 and lon>=16.0: return "NO4"
    if lat>=63.2: return "NO3"
    if lat<60.7 and lon<9.5:    return "NO2"
    if lat<61.5 and lon>=9.5:   return "NO1"
    return "NO5"
def zone_it(lat,lon):
    if 12.3<=lon<=15.7 and 36.3<=lat<=38.7: return "IT-SICI"
    if 8.0<=lon<=9.9 and 38.8<=lat<=41.6:  return "IT-SARD"
    if lat>=44.4: return "IT-NORD"
    if lat>=43.3: return "IT-CNOR"
    if lat>=41.8: return "IT-CSUD"
    return "IT-SUD"
def zone_es(lat,lon):
    if 1.0<=lon<=5.0 and 38.5<=lat<=40.2: return "ES-NPT"
    if -18.5<=lon<=-13.0 and 27.0<=lat<=29.5: return "ES-NPT"
    if -5.45<=lon<=-5.25 and 35.85<=lat<=35.95: return "ES-NPT"
    if -3.10<=lon<=-2.80 and 35.20<=lat<=35.35: return "ES-NPT"
    return "ES-MAIN"

def auto_zone_and_eic(lat: float, lon: float, cc: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if cc=="DK": z=zone_dk(lat,lon);  return z, EIC_BY_ZONE.get(z), None
    if cc=="SE": z=zone_se(lat,lon);  return z, EIC_BY_ZONE.get(z), None
    if cc=="NO": z=zone_no(lat,lon);  return z, EIC_BY_ZONE.get(z), None
    if cc=="IT": z=zone_it(lat,lon);  return z, EIC_BY_ZONE.get(z), None
    if cc=="ES":
        z=zone_es(lat,lon)
        if z=="ES-MAIN": return z, EIC_BY_ZONE["ES-MAIN"], None
        else:            return z, None, "NO_DATA_SOURCE(ES_NPT)"
    return None, None, None

def coords_to_eic(lat: float, lon: float, country_code: Optional[str]=None) -> Tuple[Optional[str], Optional[str]]:
    cc=(country_code or reverse_geocode_country(lat,lon) or "").upper()
    if not cc: return None, None
    if cc in SINGLE_ZONE_EIC and cc!="ES": return SINGLE_ZONE_EIC[cc], cc
    zone_name, eic, special = auto_zone_and_eic(lat,lon,cc)
    if special: return None, cc
    if eic:     return eic, cc
    return None, cc

# ===================== Kennzahlen (Jahresmittel) =====================
def summarize_prices(df_zone: pd.DataFrame, tz_local: str, price_col: str = "ct_kWh") -> Dict[str, float]:
    """
    Liefert den zeitgewichteten Jahres-Mittelwert (ct/kWh) und Stunden gesamt.
    """
    if df_zone.empty or price_col not in df_zone.columns:
        return {"hours_total": np.nan, "mean_ct": np.nan}

    v = pd.to_numeric(df_zone[price_col], errors="coerce").to_numpy()
    w = pd.to_numeric(df_zone["minutes"],  errors="coerce").to_numpy()
    mask = ~(np.isnan(v) | np.isnan(w))
    if not mask.any():
        return {"hours_total": np.nan, "mean_ct": np.nan}

    v = v[mask]; w = w[mask]
    minutes_total = float(np.nansum(w))
    mean_ct = float(np.nansum(v * w) / minutes_total) if minutes_total > 0 else np.nan
    return {"hours_total": minutes_total / 60.0, "mean_ct": mean_ct}

# ===================== Cache helpers =====================
def zone_csv_path(eic: str) -> str:
    return os.path.join(OUT_ZONES_DIR, f"{YEAR}_{eic}.csv")

_MONTH_RE = re.compile(r"(Jan|Feb|Mär|Mrz|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dez)", re.IGNORECASE)

def try_load_zone_csv(eic: str, force_refresh: bool = False) -> Optional[pd.DataFrame]:
    fn = zone_csv_path(eic)
    if force_refresh or not (CACHE_USE_EXISTING and os.path.exists(fn)):
        return None
    try:
        df = pd.read_csv(fn, sep=CSV_DELIMITER, dtype={"ct_kWh":"string","minutes":"string"})
        if "timestamp_utc" not in df.columns or "ct_kWh" not in df.columns:
            if VERBOSE: print(f"[cache] {eic}: Datei unvollständig → ignoriere")
            return None
        
        # Leere Datei
        if len(df) == 0:
            if VERBOSE: print(f"[cache] {eic}: leere Datei → neu versuchen")
            return None
            
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
        df["ct_kWh"] = pd.to_numeric(df["ct_kWh"].str.replace(",", ".").str.replace(" ", ""), errors="coerce")
        df["minutes"] = pd.to_numeric(df["minutes"], errors="coerce")
        raw = open(fn, "r", encoding="utf-8", errors="ignore").read(5000)
        corrupted = df["ct_kWh"].isna().mean() > 0.1 or bool(_MONTH_RE.search(raw))
        if corrupted or df["timestamp_utc"].isna().any() or df.empty:
            if VERBOSE: print(f"[cache] {eic}: Datei korrupt/überschrieben → neu laden")
            return None
        if VERBOSE:
            m = df["timestamp_utc"].min(); M = df["timestamp_utc"].max()
            print(f"[cache] {eic}: verwende CSV [{m.date()}..{M.date()}], n={len(df)}")
        return df[["timestamp_utc","ct_kWh","minutes"]].copy()
    except Exception as ex:
        if VERBOSE: print(f"[cache] {eic}: konnte nicht laden ({ex}) → neu laden")
        return None

# ===================== Hauptlauf =====================
def main():
    if not API_TOKEN or API_TOKEN.startswith("HIER_") or len(API_TOKEN) < 20:
        raise SystemExit("Bitte API_TOKEN oben setzen.")
    os.makedirs(OUT_ZONES_DIR, exist_ok=True)

    print(f"=== Zonenmodus {DATE_FROM} → {DATE_TO} (Jahr {YEAR}, {TZ_LOCAL}) — Eurostat (Band={EUROSTAT_BAND}) + VAT + Cache ===")
    print(f"Excel: {EXCEL_PATH} | General: '{SHEET_GENERAL}' → Grid Energy Connection: '{SHEET_ENERGY}'")

    assert_excel_readable(EXCEL_PATH, SHEET_GENERAL)
    sur = load_surcharges()

    df_in = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_GENERAL, engine="openpyxl")
    
    # Finde relevante Spalten
    col_lat = pick_first_existing_column(df_in, [LAT_COL,"lat","latitude"])
    col_lon = pick_first_existing_column(df_in, [LON_COL,"lon","longitude","Lng","Long"])
    if not col_lat or not col_lon:
        raise SystemExit(f"Spalten '{LAT_COL}'/'{LON_COL}' nicht gefunden.")
    id_col   = pick_first_existing_column(df_in, ID_COL_CANDIDATES)
    eic_col  = pick_first_existing_column(df_in, EIC_OVERRIDE_COL_CANDIDATES)
    country_col = pick_first_existing_column(df_in, ["CountryCode","Country","ISO2"])
    
    # Filtere leere Zeilen (wo ID oder Koordinaten fehlen)
    if id_col:
        df_in = df_in[df_in[id_col].notna()].reset_index(drop=True)
    else:
        # Wenn keine ID-Spalte, filtere nach Koordinaten
        df_in = df_in[(df_in[col_lat].notna()) | (df_in[col_lon].notna())].reset_index(drop=True)
    
    n = len(df_in)
    if VERBOSE:
        print(f"[i] {n} Anlagen im Sheet '{SHEET_GENERAL}' gefunden (nach Filterung leerer Zeilen)")

    # 1) Mapping Koordinaten → (EIC, ISO2)
    print("[1/3] Mappe Koordinaten → EIC …")
    site_eic: Dict[int, Optional[str]] = {}
    site_cc : Dict[int, Optional[str]] = {}
    for idx in pbar(range(n), desc="EIC mapping"):
        lat = df_in.at[idx, col_lat]; lon = df_in.at[idx, col_lon]
        
        # Ländercode aus Tabelle holen (falls vorhanden)
        cc_from_table = None
        if country_col and pd.notna(df_in.at[idx, country_col]):
            cc_from_table = str(df_in.at[idx, country_col]).strip().upper()[:2]
            cc_from_table = _EUROSTAT_GEO_FIX.get(cc_from_table, cc_from_table)
        
        # Wenn Koordinaten fehlen, nutze Ländercode aus Tabelle (falls vorhanden)
        if pd.isna(lat) or pd.isna(lon):
            site_eic[idx] = None
            site_cc[idx] = cc_from_table
            # Versuche EIC aus Ländercode zu ermitteln (für Single-Zone-Länder)
            if cc_from_table and cc_from_table in SINGLE_ZONE_EIC:
                site_eic[idx] = SINGLE_ZONE_EIC[cc_from_table]
            continue
            
        # Mit Koordinaten: EIC aus Koordinaten oder Tabelle
        if eic_col and pd.notna(df_in.at[idx, eic_col]):
            site_eic[idx] = str(df_in.at[idx, eic_col]).strip()
            site_cc[idx] = cc_from_table
        else:
            eic, cc_used = coords_to_eic(float(lat), float(lon), cc_from_table)
            site_eic[idx] = eic
            site_cc[idx] = cc_used or cc_from_table
            
            # Fallback: Wenn Reverse Geocoding fehlschlägt, extrahiere Land aus UWWTD Code
            if not site_cc[idx] and id_col:
                uwwtd_code = str(df_in.at[idx, id_col]).strip()
                # UWWTD Code Format: z.B. "ES4070400012010E", "IETP_D0033", "UKENTH_TWU_TP000125"
                # Erste 2 Buchstaben = Ländercode
                if len(uwwtd_code) >= 2:
                    cc_from_code = uwwtd_code[:2].upper()
                    # Validiere Buchstaben
                    if cc_from_code.isalpha():
                        site_cc[idx] = cc_from_code
                        if VERBOSE:
                            print(f"[i] Anlage {idx} ({uwwtd_code}): Ländercode aus UWWTD Code extrahiert: {cc_from_code}")

    unique_eics = sorted({e for e in site_eic.values() if isinstance(e,str)})
    print(f"    {len(unique_eics)} einzigartige Zonen werden für {YEAR} verarbeitet …")

    # 2) Preise je Zone (Jahr) — Deutschland aus CSV, Rest aus API
    eic_to_df: Dict[str, pd.DataFrame] = {}
    failed_eics = load_failed_eics()
    new_failed_eics = set()
    
    # Deutschland: CSV laden (falls vorhanden)
    de_eic = "10Y1001A1001A83F"  # DE-LU
    if de_eic in unique_eics and os.path.exists(DE_PRICES_CSV):
        print(f"[CSV] Lade Deutschland aus CSV für {YEAR} …")
        df_de = load_de_prices_from_csv(DE_PRICES_CSV, YEAR)
        if not df_de.empty:
            eic_to_df[de_eic] = df_de
            # Speichere auch als Cache
            fn = zone_csv_path(de_eic)
            out = df_de.copy(); out.insert(0, "EIC", de_eic)
            out.to_csv(fn, index=False, sep=CSV_DELIMITER)
            if VERBOSE: print(f"[save] {de_eic}: CSV-Daten gespeichert → {fn}")
        else:
            if VERBOSE: print(f"[WARN] {de_eic}: CSV leer oder fehlerhaft")
    
    # Andere Zonen: ENTSO-E API
    for eic in unique_eics:
        if eic == de_eic and de_eic in eic_to_df:
            continue  # Deutschland bereits aus CSV geladen
            
        if eic in KNOWN_NO_A44:
            print(f"[i] {eic}: A44 nicht verfügbar – übersprungen")
            continue

        # EIC neu versuchen
        force_refresh = eic in failed_eics or eic in FORCE_REFRESH_EICS
        if force_refresh and VERBOSE:
            print(f"[i] {eic}: beim letzten Lauf fehlgeschlagen → neu versuchen")
        
        use_cache = not force_refresh
        dfz = try_load_zone_csv(eic, force_refresh=force_refresh) if use_cache else None

        if dfz is None:
            print(f"[DL] Lade ENTSO-E {eic} für {YEAR} …")
            dfz = entsoe_range(eic, DATE_FROM, DATE_TO, TZ_LOCAL)
            if dfz.empty and eic in EIC_FALLBACK:
                alt = EIC_FALLBACK[eic]
                print(f"[i] {eic}: keine A44-Daten → versuche BZN {alt} …")
                dfz = entsoe_range(alt, DATE_FROM, DATE_TO, TZ_LOCAL)
                if not dfz.empty:
                    eic = alt

            fn = zone_csv_path(eic)
            if not dfz.empty:
                dfz["timestamp_utc"] = pd.to_datetime(dfz["timestamp_utc"], utc=True)
                out = dfz.copy(); out.insert(0, "EIC", eic)
                out.to_csv(fn, index=False, sep=CSV_DELIMITER)
                if VERBOSE: print(f"[save] {eic}: gespeichert → {fn} (n={len(dfz)})")
                eic_to_df[eic] = dfz  # Nur speichern wenn Daten vorhanden
            else:
                # Keine Daten von ENTSO-E → als fehlgeschlagen markieren
                new_failed_eics.add(eic)
                pd.DataFrame(columns=["EIC","timestamp_utc","ct_kWh","minutes"]).to_csv(fn, index=False, sep=CSV_DELIMITER)
                if VERBOSE: print(f"[warn] {eic}: keine Daten – leere Datei erzeugt, wird beim nächsten Start neu versucht")
                # NICHT in eic_to_df speichern, damit Eurostat-Fallback greift
        else:
            # Cache geladen
            if not dfz.empty:
                eic_to_df[eic] = dfz
            # Cache leer → Eurostat-Fallback
    
    # Fehlgeschlagene EICs speichern
    save_failed_eics(new_failed_eics)

    # ALL-IN Jahresmittel - GB Festwert
    print("[2/3] Berechne ALL-IN Jahresmittel je Anlage …")
    stats_cache: Dict[Tuple[str,str], Dict[str,float]] = {}
    per_site_stats: Dict[int, Dict[str, float]] = {}
    eurostat_cache: Dict[str, Optional[float]] = {}  # Cache für Eurostat-Preise

    used_cc = sorted({(site_cc.get(i) or "??") for i in range(n)})
    if VERBOSE:
        print("[i] genutzte Länder & Aufschläge (ct/kWh + VAT%):")
        for cc in used_cc:
            cfg = sur.get(cc, sur["DEFAULT"])
            print(f"    {cc}: grid={cfg['grid_fee_ct']:.3f}, levies={cfg['levies_ct']:.3f}, tax={cfg['tax_ct']:.3f}, VAT={cfg['vat_pct']:.1f}%")

    for idx in pbar(range(n), desc="Per-site mean"):
        eic = site_eic.get(idx)
        cc = (site_cc.get(idx) or "").upper()
        
        # Wenn weder EIC noch Land vorhanden → leerer Eintrag
        if not eic and not cc:
            per_site_stats[idx] = {
                "EIC": None, "Country": None,
                "mean_ct": np.nan
            }
            if VERBOSE:
                print(f"[WARN] Anlage {idx}: keine Koordinaten und kein Ländercode → keine Preisberechnung möglich")
            continue

        # --- GB: Festwert verwenden ---
        if USE_GB_FIXED_MEAN and cc == "GB":
            cfg = sur.get("GB", sur["DEFAULT"])
            allin_ct = (GB_SPOT_CT + cfg["grid_fee_ct"] + cfg["levies_ct"] + cfg["tax_ct"]) * (1.0 + cfg["vat_pct"]/100.0)
            per_site_stats[idx] = {
                "EIC": eic, "Country": cc,
                "mean_ct": float(allin_ct)
            }
            continue

        # Standardweg (Zeitreihe)
        if not (isinstance(eic, str) and eic in eic_to_df and not eic_to_df[eic].empty):
            # Keine ENTSO-E Daten → Eurostat-Fallback versuchen
            if cc and cc != "??":
                if cc not in eurostat_cache:
                    eurostat_price = fetch_eurostat_annual_price(cc, YEAR, band_code=EUROSTAT_BAND)
                    eurostat_cache[cc] = eurostat_price
                    if eurostat_price is not None and VERBOSE:
                        print(f"[Eurostat] {cc}: Jahresmittel {eurostat_price:.2f} ct/kWh (I_TAX: ohne MwSt)")
                else:
                    eurostat_price = eurostat_cache[cc]
                
                if eurostat_price is not None:
                    # Eurostat-Preis ohne MwSt → MwSt hinzufügen
                    cfg = sur.get(cc, sur["DEFAULT"])
                    eurostat_allin = eurostat_price * (1.0 + cfg["vat_pct"]/100.0)
                    per_site_stats[idx] = {
                        "EIC": eic, "Country": cc,
                        "mean_ct": float(eurostat_allin)
                    }
                    continue
            
            # Kein Eurostat-Preis verfügbar
            per_site_stats[idx] = {"EIC": eic, "Country": cc, "mean_ct": np.nan}
            continue

        key = (eic, cc)
        if key not in stats_cache:
            dfz = eic_to_df[eic].copy()
            cfg = sur.get(cc, sur["DEFAULT"])
            def apply_allin(x_ct: float) -> float:
                base = float(x_ct) + cfg["grid_fee_ct"] + cfg["levies_ct"] + cfg["tax_ct"]
                return base * (1.0 + cfg["vat_pct"]/100.0)
            dfz["allin_ct_kWh"] = pd.to_numeric(dfz["ct_kWh"], errors="coerce").apply(apply_allin)
            s = summarize_prices(dfz, TZ_LOCAL, price_col="allin_ct_kWh")
            stats_cache[key] = s

        per_site_stats[idx] = {
            "EIC": eic, "Country": cc,
            "mean_ct": stats_cache[key]["mean_ct"]
        }

    # Fallback: Länderdurchschnitt
    print("[2.5/3] Fülle fehlende Preise mit Länderdurchschnitt auf …")
    
    # Durchschnitt pro Land berechnen
    country_averages: Dict[str, float] = {}
    for idx in range(n):
        stats = per_site_stats.get(idx, {})
        cc = stats.get("Country")
        price = stats.get("mean_ct")
        
        if cc and pd.notna(price) and not np.isnan(price):
            if cc not in country_averages:
                country_averages[cc] = []
            country_averages[cc].append(price)
    
    # Mittelwert berechnen
    for cc in country_averages:
        country_averages[cc] = float(np.mean(country_averages[cc]))
        if VERBOSE:
            print(f"    {cc}: Durchschnitt = {country_averages[cc]:.2f} ct/kWh ({len([idx for idx in range(n) if per_site_stats.get(idx, {}).get('Country') == cc and pd.notna(per_site_stats.get(idx, {}).get('mean_ct'))])} Anlagen)")
    
    # Fülle fehlende Werte mit Länderdurchschnitt
    filled_count = 0
    for idx in range(n):
        stats = per_site_stats.get(idx, {})
        cc = stats.get("Country")
        price = stats.get("mean_ct")
        
        if cc and (pd.isna(price) or np.isnan(price)) and cc in country_averages:
            per_site_stats[idx]["mean_ct"] = country_averages[cc]
            filled_count += 1
            if VERBOSE:
                plant_name = df_in.at[idx, "Name"] if "Name" in df_in.columns else f"Anlage {idx}"
                print(f"    ✓ {plant_name} ({cc}): Preis aufgefüllt mit Länderdurchschnitt {country_averages[cc]:.2f} ct/kWh")
    
    if filled_count > 0:
        print(f"    → {filled_count} Anlagen mit Länderdurchschnitt aufgefüllt")

    # 4) Excel-Sheet "Grid Energy Connection" (nur Jahresmittelspalte)
    print("[3/3] Aktualisiere Excel-Sheet 'Grid Energy Connection' …")
    try:
        df_energy = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_ENERGY, engine="openpyxl")
    except Exception:
        df_energy = pd.DataFrame()

    ids_general = df_in[id_col] if id_col else pd.Series(range(n), name="ID")
    def align_energy(df_energy):
        id_energy = pick_first_existing_column(df_energy, ID_COL_CANDIDATES) if not df_energy.empty else None
        if df_energy.empty:
            return pd.DataFrame({(id_col or "ID"): ids_general}), (id_col or "ID")
        if id_col and id_energy:
            merged = pd.merge(ids_general.to_frame(), df_energy, how="left",
                              left_on=id_col, right_on=id_energy)
            if id_energy != id_col and id_energy in merged.columns: merged.drop(columns=[id_energy], inplace=True)
            return merged, id_col
        else:
            df_energy = df_energy.reindex(range(n)).reset_index(drop=True)
            if id_col and id_col not in df_energy.columns: df_energy.insert(0, id_col, ids_general)
            return df_energy, (id_col or "ID")

    df_energy, id_col_energy = align_energy(df_energy)

    suffix = f" {YEAR}"
    colmap = {
        "EIC": ("EIC", "string"),
        f"Annual Average Price [ct/kWh]{suffix}": ("mean_ct", "float"),   # ALL-IN
    }

    for newcol, (_, kind) in colmap.items():
        if newcol not in df_energy.columns:
            df_energy[newcol] = pd.Series(index=df_energy.index, dtype=("string" if kind=="string" else "Float64"))

    for idx in range(n):
        s = per_site_stats.get(idx, {})
        for newcol, (key, kind) in colmap.items():
            val = s.get(key, np.nan)
            if kind == "string":
                df_energy.at[idx, newcol] = None if (val is np.nan or pd.isna(val)) else str(val)
            else:
                df_energy.at[idx, newcol] = np.nan if (val is None or (isinstance(val, str) and val=="")) else float(val)

    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl", mode="a", if_sheet_exists="replace") as xw:
        df_energy.to_excel(xw, index=False, sheet_name=SHEET_ENERGY)

    print("Fertig ✅")
    print(f"  • Zonen-CSV (Cache): {OUT_ZONES_DIR} (Schema: {YEAR}_<EIC>.csv)")
    print(f"  • KPI: 'Annual Average Price [ct/kWh]{suffix}' (ALL-IN) im Sheet '{SHEET_ENERGY}'.")
    if USE_GB_FIXED_MEAN:
        print(f"  • GB fix: spot={GB_SPOT_CT:.4f} ct/kWh, DUoS={GB_DUOS_HV_CT:.4f} ct/kWh, BSUoS={GB_BSUOS_CT:.4f} ct/kWh, CCL={GB_CCL_CT:.4f} ct/kWh, VAT={GB_VAT_PCT:.1f}%")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAbgebrochen.")
        sys.exit(1)
