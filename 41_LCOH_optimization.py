import numpy as np
import pandas as pd
from scipy.optimize import minimize
from pathlib import Path
import re
import warnings
import time
import sys
import pickle
import hashlib

# Warnungen ausschalten
warnings.filterwarnings('ignore')

# Einstellungen

# I/O
INPUT_FILE = "Output/UWWTD_TP_Database.xlsx"
OUTPUT_FILE = "Output/H2_cost_optimization_results_optimizer.xlsx"
SHEET_NAME = "General Data"

# Länderdaten
COUNTRY_DATA_FILE = "Daten/EU_PV_Wind_CAPEX_OPEX_WACC_DEref.xlsx"

# Cache
CACHE_FILE = "Output/H2_optimization_cache.pkl"

# Pfade für Zeitreihen
ROOT = Path(__file__).resolve().parent
TIMESERIES_BASE = ROOT / "Output" / "RenewableEnergyProfiles"
PV_DIR = TIMESERIES_BASE / "PV"
WIND_DIR = TIMESERIES_BASE / "Wind"

# Finanzrechnung

def crf(wacc: float, lifetime: int) -> float:
    """Capital Recovery Factor"""
    r = wacc
    n = lifetime
    return r * (1 + r)**n / ((1 + r)**n - 1)

# Wasserstoff-Konstanten
LHV_H2 = 33.33  # kWh/kg (Lower Heating Value)

# PEM-Elektrolyse Effizienz
X_ETA = np.array([0.10, 0.20, 0.30, 0.40, 1.00])  # Last [-]
Y_ETA = np.array([0.400, 0.470, 0.525, 0.525, 0.500])  # η_LHV [-]

# ULTRA-PERFORMANCE: Lookup-Tabelle für direkte Array-Zugriffe
_ETA_LOOKUP_TABLE = None
_ETA_LOAD_GRID = None
_ETA_TABLE_RESOLUTION = 1000  # Anzahl Stützpunkte in Lookup-Tabelle

def _create_eta_lookup_table():
    """
    Erstellt einmalig eine hochauflösende Lookup-Tabelle für η(load).
    Danach nur noch direkte Array-Zugriffe → EXTREM schnell!
    """
    global _ETA_LOOKUP_TABLE, _ETA_LOAD_GRID
    
    if _ETA_LOOKUP_TABLE is not None:
        return  # Bereits erstellt
    
    print("Creating efficiency lookup table for ultra-fast access...")
    
    # Hochauflösendes Load-Grid von 0.10 bis 1.00
    _ETA_LOAD_GRID = np.linspace(0.10, 1.00, _ETA_TABLE_RESOLUTION)
    
    # Einmalig alle η-Werte interpolieren
    try:
        # Versuche SciPy PCHIP (beste Qualität)
        from scipy.interpolate import PchipInterpolator
        interp = PchipInterpolator(X_ETA, Y_ETA)
        _ETA_LOOKUP_TABLE = interp(_ETA_LOAD_GRID)
        method = "SciPy PCHIP"
    except ImportError:
        # Fallback: Lineare Interpolation (immer noch sehr gut)
        _ETA_LOOKUP_TABLE = np.interp(_ETA_LOAD_GRID, X_ETA, Y_ETA)
        method = "Linear"
    
    # Sicherheitsgrenze anwenden
    _ETA_LOOKUP_TABLE = np.clip(_ETA_LOOKUP_TABLE, 0.05, 0.95)
    
    print(f"✓ Lookup table created: {_ETA_TABLE_RESOLUTION} points, method: {method}")
    print(f"  Load range: {_ETA_LOAD_GRID[0]:.3f} - {_ETA_LOAD_GRID[-1]:.3f}")
    print(f"  η range: {_ETA_LOOKUP_TABLE.min():.4f} - {_ETA_LOOKUP_TABLE.max():.4f}")


def _eta_lookup_fast(load):
    """
    Ultra-schneller η-Lookup über vorberechnete Tabelle.
    Nutzt np.searchsorted + lineare Interpolation zwischen Tabellenpunkten.
    """
    # Sicherstellen dass Tabelle existiert
    if _ETA_LOOKUP_TABLE is None:
        _create_eta_lookup_table()
    
    # Last auf gültigen Bereich clampen
    load_clamped = np.clip(load, 0.10, 1.00)
    
    # Index in Lookup-Tabelle finden
    idx = np.searchsorted(_ETA_LOAD_GRID, load_clamped)
    idx = np.clip(idx, 1, len(_ETA_LOAD_GRID) - 1)
    
    # Lineare Interpolation zwischen benachbarten Tabellenpunkten
    load_low = _ETA_LOAD_GRID[idx - 1]
    load_high = _ETA_LOAD_GRID[idx]
    eta_low = _ETA_LOOKUP_TABLE[idx - 1]
    eta_high = _ETA_LOOKUP_TABLE[idx]
    
    # Interpolationsfaktor
    t = (load_clamped - load_low) / (load_high - load_low)
    
    # Interpolierte Effizienz
    eta = eta_low + t * (eta_high - eta_low)
    
    return eta


def eta_from_load(load: float, e_spec_nom: float = 50.0) -> float:
    """
    Absolute LHV-Effizienz des PEM-Elektrolyseurs als Funktion der relativen Last.
    
    ULTRA-PERFORMANCE: Nutzt vorberechnete Lookup-Tabelle für maximale Geschwindigkeit.
    Einmalige Interpolation beim ersten Aufruf, danach nur Array-Zugriffe.
    
    Parameter:
    - load: Relative Last = p_ely / P_ely_nom [0...1]
    - e_spec_nom: Spezifischer Stromverbrauch bei Nennlast [kWh/kg H2] (nicht verwendet)
    
    Returns:
    - eta: Absolute LHV-Effizienz [0...1], wobei eta = LHV / e_spec
    
    PEM-System Effizienz-Charakteristik (LHV-basiert):
    - Bei load = 0.10: η = 0.400 (40.0%, e_spec = 83.3 kWh/kg)
    - Bei load = 0.20: η = 0.470 (47.0%, e_spec = 70.9 kWh/kg)
    - Bei load = 0.30: η = 0.525 (52.5%, e_spec = 63.5 kWh/kg)
    - Bei load = 0.40: η = 0.525 (52.5%, e_spec = 63.5 kWh/kg) - Plateau
    - Bei load = 1.00: η = 0.500 (50.0%, e_spec = 66.7 kWh/kg)
    
    Hinweis: η ist LHV-Effizienz; LHV = 33.33 kWh/kg
    """
    # ULTRA-PERFORMANCE: Lookup-Tabelle (extrem schnell!)
    return _eta_lookup_fast(load)
    
    # Alternative Methoden (alle langsamer):
    # 
    # # Einfache lineare Interpolation
    # load_clamped = np.clip(load, 0.10, 1.00)
    # eta = np.interp(load_clamped, X_ETA, Y_ETA)
    # return np.clip(eta, 0.05, 0.95)
    #
    # # PCHIP-Interpolation (glatter, aber viel langsamer)
    # method, interpolator = _get_eta_interpolator()
    # if method == 'scipy':
    #     eta = float(interpolator(load_clamped))
    # else:
    #     eta = _pchip_eval_numpy(load_clamped, interpolator)
    # return np.clip(eta, 0.05, 0.95)


def e_spec_from_eta(eta: float) -> float:
    """
    Spezifischer Stromverbrauch aus LHV-Effizienz.
    
    Parameter:
    - eta: Absolute LHV-Effizienz [0...1]
    
    Returns:
    - e_spec: Spezifischer Stromverbrauch [kWh/kg H2]
    
    Formel: e_spec = LHV / eta
    """
    if eta <= 0:
        return np.inf
    
    return LHV_H2 / eta


def e_spec_from_load(load: float, e_spec_nom: float = 50.0) -> float:
    """
    Spezifischer Stromverbrauch des PEM-Elektrolyseurs [kWh/kg H2]
    als Funktion der relativen Last 'load' = p_ely / P_ely_nom.
    
    Konsistente LHV-basierte Berechnung:
    load → eta(load) → e_spec = LHV / eta
    
    Parameter:
    - load: Relative Last [0...1]
    - e_spec_nom: Spezifischer Stromverbrauch bei Nennlast [kWh/kg H2]
    
    Returns:
    - e_spec: Spezifischer Stromverbrauch [kWh/kg H2]
    """
    eta = eta_from_load(load, e_spec_nom)
    return e_spec_from_eta(eta)


def simulate_pem_electrolyzer(
    pv_norm: np.ndarray,
    wind_norm: np.ndarray,
    alpha: float,
    k: float,
    dt_hours: float = 1.0,
) -> dict:
    """
    Realistisches PEM-Elektrolyseur Systemmodell.
    
    Parameter:
    - pv_norm, wind_norm: Zeitreihen (0..1) pro 1 kW installierter Leistung
    - alpha: PV-Anteil in der installierten EE-Leistung [0,1]
    - k: Überdimensionierungsfaktor EE-Park relativ zur Ely-Nennleistung [1,5]
    - dt_hours: Zeitschritt (1.0 h)
    
    Returns: Dict mit Ergebnissen
    """
    # PEM-Elektrolyseur Parameter (Systemebene)
    P_ely_nom = 1.0  # kW (normiert)
    P_min_op = 0.1 * P_ely_nom  # 10% Mindestlast
    P_standby = 0.01 * P_ely_nom  # 1% Standby-Leistung
    E_start = P_ely_nom * (5/60)  # 5 Minuten Volllast bei jedem Start (kWh)
    
    # EE-Park Dimensionierung
    C_res = k * P_ely_nom  # kW EE gesamt
    C_pv = alpha * C_res   # kW PV
    C_wind = (1 - alpha) * C_res  # kW Wind
    
    # Stündliche EE-Erzeugung
    p_pv = C_pv * pv_norm      # kW
    p_wind = C_wind * wind_norm  # kW
    p_res = p_pv + p_wind      # kW
    
    # Elektrolyseur-Betrieb simulieren
    n_hours = len(pv_norm)
    p_ely = np.zeros(n_hours)
    E_el_hourly = np.zeros(n_hours)
    m_H2_hourly = np.zeros(n_hours)
    E_start_tot = 0.0
    
    for t in range(n_hours):
        # Elektrolyseur-Leistung bestimmen
        if p_res[t] >= P_min_op:
            p_ely[t] = min(P_ely_nom, p_res[t])
        else:
            p_ely[t] = 0.0
        
        # Start-Erkennung: Übergang von OFF zu Produktionsbetrieb
        if t > 0 and p_ely[t-1] < P_min_op and p_ely[t] >= P_min_op:
            E_start_tot += E_start
        
        # Elektrischer Verbrauch in der Stunde
        if p_ely[t] >= P_min_op:
            E_el_hourly[t] = p_ely[t] * dt_hours  # kWh
            
            # H2-Produktion mit Teillast-Wirkungsgrad
            load = p_ely[t] / P_ely_nom
            e_spec = e_spec_from_load(load)  # kWh/kg H2
            m_H2_hourly[t] = p_ely[t] * dt_hours / e_spec  # kg
        else:
            # Elektrolyseur OFF: Kein Standby-Verbrauch
            E_el_hourly[t] = 0.0  # kWh (komplett aus)
            m_H2_hourly[t] = 0.0
    
    # Summen über alle Stunden
    E_el_tot = np.sum(E_el_hourly) + E_start_tot  # kWh, inkl. Starts
    m_H2_tot = np.sum(m_H2_hourly)  # kg
    
    # Auf Jahresbasis normieren
    total_hours = len(pv_norm) * dt_hours
    n_years = total_hours / 8760.0
    
    E_el_year = E_el_tot / n_years  # kWh/a
    m_H2_year = m_H2_tot / n_years  # kg/a
    FLH_ely = E_el_year / P_ely_nom  # h/a
    
    # Weitere Kennzahlen
    E_pv_year = np.sum(p_pv) * dt_hours / n_years
    E_wind_year = np.sum(p_wind) * dt_hours / n_years
    
    return {
        "C_pv": C_pv,
        "C_wind": C_wind,
        "P_ely_nom": P_ely_nom,
        "E_pv_year": E_pv_year,
        "E_wind_year": E_wind_year,
        "E_el_year": E_el_year,
        "m_H2_year": m_H2_year,
        "FLH_ely": FLH_ely,
        "E_start_tot": E_start_tot,
        "n_starts": np.sum((p_ely[1:] > 0) & (p_ely[:-1] == 0)),  # Anzahl Starts
    }


def compute_annual_costs_EE(
    C_pv: float,
    C_wind: float,
    capex_pv: float,
    opex_pv_fix: float,
    wacc_pv: float,
    lifetime_pv: int,
    capex_wind: float,
    opex_wind_fix: float,
    wacc_wind: float,
    lifetime_wind: int,
) -> float:
    """Jährliche Kosten der EE-Anlage (PV+Wind) in €/a."""
    crf_pv = crf(wacc_pv, lifetime_pv)
    crf_wind = crf(wacc_wind, lifetime_wind)
    
    K_pv_ann = capex_pv * C_pv * crf_pv + opex_pv_fix * C_pv
    K_wind_ann = capex_wind * C_wind * crf_wind + opex_wind_fix * C_wind
    
    return K_pv_ann + K_wind_ann


def compute_annual_costs_ely(
    P_ely_nom: float,
    capex_ely: float = 1800.0,
    opex_ely_fix: float = 54.0,
    wacc_ely: float = 0.065,
    lifetime_ely: int = 15,
) -> float:
    """Jährliche Kosten des PEM-Elektrolyseurs in €/a."""
    crf_ely = crf(wacc_ely, lifetime_ely)
    K_ely_capex_ann = capex_ely * P_ely_nom * crf_ely
    K_ely_opex_fix = opex_ely_fix * P_ely_nom
    return K_ely_capex_ann + K_ely_opex_fix


# ---------- Zeitreihen-Funktionen ----------

def read_timeseries_csv(csv_path: Path) -> pd.Series:
    """Liest eine Zeitreihen-CSV und gibt die 'electricity'-Spalte zurück."""
    lines = []
    with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if not line.startswith("#"):
                lines.append(line)
    
    if not lines:
        return pd.Series(dtype=float)
    
    tmp = csv_path.parent / "_tmp_read.csv"
    tmp.write_text("".join(lines), encoding="utf-8")
    
    try:
        df = pd.read_csv(tmp)
        if "electricity" not in df.columns:
            return pd.Series(dtype=float)
        return df["electricity"]
    finally:
        try:
            tmp.unlink()
        except Exception:
            pass


def find_matching_files(site_name: str) -> tuple:
    """Findet die passenden PV- und Wind-CSV-Dateien für einen Standort."""
    # Sanitize name
    sanitized_name = re.sub(r"[^\w\-]+", "_", site_name)
    sanitized_name = re.sub(r"_+", "_", sanitized_name).strip("_")
    sanitized_name = sanitized_name[:120] or "site"
    
    pv_files = list(PV_DIR.glob(f"PV_{sanitized_name}_*.csv"))
    wind_files = list(WIND_DIR.glob(f"Wind_{sanitized_name}_*.csv"))
    
    pv_csv = pv_files[0] if pv_files else None
    wind_csv = wind_files[0] if wind_files else None
    
    return pv_csv, wind_csv


def print_progress_bar(iteration, total, prefix='', suffix='', length=50, fill='█', print_end="\r"):
    """
    Erstellt einen  Ladebalken.
    """
    percent = f"{100 * (iteration / float(total)):.1f}"
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)
    if iteration == total:
        print()


def print_site_status(counter, total, site_name, status, elapsed_time=None):
    """
    Zeigt detaillierten Status für aktuelle Anlage (self-refreshing).
    """
    # Ladebalken
    percent = f"{100 * (counter / float(total)):.1f}"
    filled_length = int(50 * counter // total)
    bar = '█' * filled_length + '-' * (50 - filled_length)
    
    # Detaillierter Status in einer Zeile
    site_short = site_name[:35] + "..." if len(site_name) > 35 else site_name
    
    if elapsed_time:
        line = f"\rProgress |{bar}| {percent}% ({counter}/{total}) | {site_short:<40} | {status} ({elapsed_time:.1f}s)"
    else:
        line = f"\rProgress |{bar}| {percent}% ({counter}/{total}) | {site_short:<40} | {status}"
    
    # Zeile überschreiben, nicht neue Zeile
    print(line, end='', flush=True)


def create_cache_key(site_name: str, country_params: dict, flh_min: float) -> str:
    """Erstellt einen eindeutigen Cache-Key für eine Anlage."""
    # Kombiniere relevante Parameter für Hash
    key_data = f"{site_name}_{flh_min}_{str(sorted(country_params.items()))}"
    return hashlib.md5(key_data.encode()).hexdigest()


def load_cache() -> dict:
    """Lädt den Cache aus der Datei."""
    try:
        if Path(CACHE_FILE).exists():
            with open(CACHE_FILE, 'rb') as f:
                cache = pickle.load(f)
            print(f"✓ Cache loaded: {len(cache)} entries from {CACHE_FILE}")
            return cache
        else:
            print(f"No cache file found, starting fresh")
            return {}
    except Exception as e:
        print(f"Warning: Could not load cache: {e}")
        return {}


def save_cache(cache: dict):
    """Speichert den Cache in die Datei."""
    try:
        # Erstelle Output-Ordner falls nicht vorhanden
        Path(CACHE_FILE).parent.mkdir(parents=True, exist_ok=True)
        
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(cache, f)
        print(f"✓ Cache saved: {len(cache)} entries to {CACHE_FILE}")
    except Exception as e:
        print(f"Warning: Could not save cache: {e}")


def load_country_parameters() -> dict:
    """Lädt die länderspezifischen CAPEX/OPEX/WACC Parameter aus Excel."""
    try:
        df = pd.read_excel(COUNTRY_DATA_FILE)
        
        # Mapping von Ländernamen zu UWWTD Codes
        country_mapping = {
            "Austria": "ATTP",
            "Belgium": "BETP", 
            "Bulgaria": "BGTP",
            "Croatia": "HRTP",
            "Cyprus": "CYTP",
            "Czechia": "CZTP",
            "Denmark": "DKTP",
            "Estonia": "EETP",
            "Finland": "FITP",
            "France": "FRTP",
            "Germany": "DETP",
            "Greece": "GRTP",
            "Hungary": "HUTP",
            "Ireland": "IETP",
            "Italy": "ITTP",
            "Latvia": "LVTP",
            "Lithuania": "LTTP",
            "Luxembourg": "LUTP",
            "Malta": "MTTP",
            "Netherlands": "NLTP",
            "Poland": "PLTP",
            "Portugal": "PTTP",
            "Romania": "ROTP",
            "Slovakia": "SKTP",
            "Slovenia": "SITP",
            "Spain": "ESTP",
            "Sweden": "SETP",
            "United Kingdom": "UKTP",
        }
        
        country_params = {}
        
        for _, row in df.iterrows():
            country_name = row["Country"]
            if country_name in country_mapping:
                country_code = country_mapping[country_name]
                
                # WACC in Dezimal umwandeln (von % zu Dezimal)
                wacc_pv = row["WACC_PV_2021_real_after_tax_%"] / 100.0
                wacc_wind = row["WACC_Wind_2021_real_after_tax_%"] / 100.0
                
                country_params[country_code] = {
                    "CAPEX_PV": float(row["PV_CAPEX_EUR_per_kW"]),
                    "OPEX_PV_FIX": float(row["PV_O&M_EUR_per_kWyr"]),
                    "WACC_PV": wacc_pv,
                    "LIFETIME_PV": 30,  # Standard-Annahme
                    "CAPEX_WIND": float(row["Wind_CAPEX_EUR_per_kW"]),
                    "OPEX_WIND_FIX": float(row["Wind_O&M_EUR_per_kWyr"]),
                    "WACC_WIND": wacc_wind,
                    "LIFETIME_WIND": 25,  # Standard-Annahme
                }
        
        print(f"✓ Loaded country parameters for {len(country_params)} countries")
        return country_params
        
    except Exception as e:
        print(f"Warning: Could not load country data from {COUNTRY_DATA_FILE}: {e}")
        print("Using fallback parameters...")
        
        # Fallback zu Deutschland als Standard
        return {
            "DETP": {
                "CAPEX_PV": 677.0,
                "OPEX_PV_FIX": 13.5,
                "WACC_PV": 0.013,
                "LIFETIME_PV": 30,
                "CAPEX_WIND": 1350.0,
                "OPEX_WIND_FIX": 40.5,
                "WACC_WIND": 0.013,
                "LIFETIME_WIND": 25,
            }
        }


# ---------- LCOH Objective für Optimizer ----------

def lcoh_objective(
    x,
    pv_norm,
    wind_norm,
    country_params,
    flh_min: float,
    dt_hours: float,
):
    """
    x = [alpha, k]
    Ziel: LCOH minimieren.
    Nebenbedingung: FLH_ely >= flh_min, sonst große Penalty.
    """
    alpha, k = x

    # Bounds-Schutz, falls Optimizer rausläuft
    if alpha < 0.0 or alpha > 1.0 or k < 1.0 or k > 5.0:
        return 1e9

    # PEM-Elektrolyseur Simulation
    results = simulate_pem_electrolyzer(pv_norm, wind_norm, alpha, k, dt_hours)
    
    FLH_ely = results["FLH_ely"]
    m_H2_year = results["m_H2_year"]
    
    # Nebenbedingung: Mindest-FLH
    if FLH_ely < flh_min:
        penalty = 1e6 + (flh_min - FLH_ely) * 1e3
        return penalty
    
    # Keine H2-Produktion
    if m_H2_year <= 0:
        return 1e9

    # EE-Kosten berechnen
    K_EE_ann = compute_annual_costs_EE(
        C_pv=results["C_pv"],
        C_wind=results["C_wind"],
        capex_pv=country_params["CAPEX_PV"],
        opex_pv_fix=country_params["OPEX_PV_FIX"],
        wacc_pv=country_params["WACC_PV"],
        lifetime_pv=country_params["LIFETIME_PV"],
        capex_wind=country_params["CAPEX_WIND"],
        opex_wind_fix=country_params["OPEX_WIND_FIX"],
        wacc_wind=country_params["WACC_WIND"],
        lifetime_wind=country_params["LIFETIME_WIND"],
    )

    # PEM-Elektrolyseur Kosten
    K_ELY_ann = compute_annual_costs_ely(results["P_ely_nom"])
    
    # Gesamtkosten und LCOH
    K_tot_ann = K_EE_ann + K_ELY_ann
    LCOH = K_tot_ann / m_H2_year  # €/kg H2
    
    return LCOH


# ---------- Optimierung für EINEN Standort (PEM-Modell) ----------

def grid_search(
    alpha_list: np.ndarray,
    k_list: np.ndarray,
    pv_norm: np.ndarray,
    wind_norm: np.ndarray,
    country_params: dict,
    flh_min: float = 3000.0,
    dt_hours: float = 1.0,
) -> tuple:
    """
    Allgemeine Grid-Search über gegebene alpha- und k-Listen.
    Returns: (best_alpha, best_k, best_lcoh)
    """
    best_lcoh = np.inf
    best_alpha = 0.5
    best_k = 2.0
    
    for alpha in alpha_list:
        for k in k_list:
            # LCOH-Berechnung
            results = simulate_pem_electrolyzer(pv_norm, wind_norm, alpha, k, dt_hours)
            
            # FLH-Check
            if results["FLH_ely"] < flh_min:
                continue
                
            if results["m_H2_year"] <= 0:
                continue
            
            # Kosten berechnen
            K_EE_ann = compute_annual_costs_EE(
                C_pv=results["C_pv"],
                C_wind=results["C_wind"],
                capex_pv=country_params["CAPEX_PV"],
                opex_pv_fix=country_params["OPEX_PV_FIX"],
                wacc_pv=country_params["WACC_PV"],
                lifetime_pv=country_params["LIFETIME_PV"],
                capex_wind=country_params["CAPEX_WIND"],
                opex_wind_fix=country_params["OPEX_WIND_FIX"],
                wacc_wind=country_params["WACC_WIND"],
                lifetime_wind=country_params["LIFETIME_WIND"],
            )
            
            K_ELY_ann = compute_annual_costs_ely(results["P_ely_nom"])
            K_tot_ann = K_EE_ann + K_ELY_ann
            LCOH = K_tot_ann / results["m_H2_year"]
            
            if LCOH < best_lcoh:
                best_lcoh = LCOH
                best_alpha = alpha
                best_k = k
    
    return best_alpha, best_k, best_lcoh


def two_stage_grid_search(
    pv_norm: np.ndarray,
    wind_norm: np.ndarray,
    country_params: dict,
    flh_min: float = 3000.0,
    dt_hours: float = 1.0,
) -> dict:
    """
    Zweistufige Grid-Search: Grob → Fein → Optional scipy.optimize
    Returns: Dict mit allen Zwischenergebnissen
    """
    
    # === STUFE 1: GROBES RASTER ===
    alpha_coarse = np.linspace(0, 1, 11)  # Schritt 0.1
    k_coarse = np.arange(1.0, 5.0 + 1e-9, 0.5)  # Schritt 0.5
    
    alpha0, k0, lcoh0 = grid_search(
        alpha_coarse, k_coarse, pv_norm, wind_norm, country_params, flh_min, dt_hours
    )
    
    # === STUFE 2: FEINES RASTER (LOKAL) ===
    alpha_fine = np.linspace(alpha0 - 0.10, alpha0 + 0.10, 9)  # ca. 0.025 Schritt
    k_fine = np.arange(k0 - 0.50, k0 + 0.50 + 1e-9, 0.10)  # Schritt 0.1
    
    # Auf gültige Bereiche clampen
    alpha_fine = np.clip(alpha_fine, 0.0, 1.0)
    k_fine = np.clip(k_fine, 0.5, 10.0)
    
    alpha1, k1, lcoh1 = grid_search(
        alpha_fine, k_fine, pv_norm, wind_norm, country_params, flh_min, dt_hours
    )
    
    # === STUFE 3: ADAPTIVE OPTIMIZATION (L-BFGS-B → POWELL FALLBACK) ===
    alpha_final, k_final, lcoh_final = alpha1, k1, lcoh1
    optimize_success = False
    optimize_message = "Grid search only"
    
    try:
        from scipy.optimize import minimize
        
        # Objective function wrapper
        def objective_wrapper(x):
            return lcoh_objective(x, pv_norm, wind_norm, country_params, flh_min, dt_hours)
        
        # === VERSUCH 1: L-BFGS-B (schnell, gradientenbasiert) ===
        result_lbfgs = minimize(
            objective_wrapper,
            x0=[alpha1, k1],
            method="L-BFGS-B",
            bounds=[(0.0, 1.0), (0.5, 10.0)],
            options={"maxiter": 50, "ftol": 1e-3}
        )
        
        if result_lbfgs.success and result_lbfgs.fun < lcoh1:
            # L-BFGS-B erfolgreich
            alpha_final, k_final = result_lbfgs.x
            lcoh_final = result_lbfgs.fun
            optimize_success = True
            optimize_message = f"L-BFGS-B: converged in {result_lbfgs.nit} iterations"
        else:
            # === VERSUCH 2: POWELL FALLBACK (robust, gradientenfrei) ===
            optimize_message = f"L-BFGS-B failed ({result_lbfgs.message}), trying Powell..."
            
            result_powell = minimize(
                objective_wrapper,
                x0=[alpha1, k1],
                method="Powell",
                bounds=[(0.0, 1.0), (0.5, 10.0)],
                options={"maxiter": 100, "xtol": 1e-3, "ftol": 1e-3}
            )
            
            if result_powell.success and result_powell.fun < lcoh1:
                # Powell erfolgreich
                alpha_final, k_final = result_powell.x
                lcoh_final = result_powell.fun
                optimize_success = True
                optimize_message = f"Powell fallback: {result_powell.message}"
            else:
                # Beide Optimizer fehlgeschlagen → Grid-Ergebnis behalten
                optimize_message = f"Both optimizers failed: L-BFGS-B ({result_lbfgs.message}), Powell ({result_powell.message})"
            
    except ImportError:
        optimize_message = "SciPy not available"
    except Exception as e:
        optimize_message = f"Optimization error: {str(e)}"
    
    return {
        # Stufe 1 (grob)
        "alpha_coarse": alpha0,
        "k_coarse": k0,
        "lcoh_coarse": lcoh0,
        # Stufe 2 (fein)
        "alpha_fine": alpha1,
        "k_fine": k1,
        "lcoh_fine": lcoh1,
        # Stufe 3 (final)
        "alpha_final": alpha_final,
        "k_final": k_final,
        "lcoh_final": lcoh_final,
        "optimize_success": optimize_success,
        "optimize_message": optimize_message,
        # Verbesserungen
        "improvement_coarse_to_fine": (lcoh0 - lcoh1) / lcoh0 * 100 if lcoh0 > 0 else 0,
        "improvement_fine_to_final": (lcoh1 - lcoh_final) / lcoh1 * 100 if lcoh1 > 0 else 0,
    }


def optimize_site_lcoh(
    pv_norm: np.ndarray,
    wind_norm: np.ndarray,
    country_params: dict,
    flh_min: float = 3000.0,
    dt_hours: float = 1.0,
) -> dict:
    """
    Dreistufige Optimierung: Grob → Fein → scipy.optimize
    
    Returns:
        Dict mit alpha_opt, k_opt, LCOH_min, FLH_ely_opt, m_H2_year, 
        K_EE_ann, K_ELY_ann, success, message
    """
    # dreistufige Grid-Search
    search_results = two_stage_grid_search(
        pv_norm, wind_norm, country_params, flh_min, dt_hours
    )
    
    # Falls Grid-Search kein gültiges Ergebnis findet
    if search_results["lcoh_coarse"] == np.inf:
        return {
            "alpha_opt_cost": np.nan,
            "k_opt_cost": np.nan,
            "LCOH_min": np.inf,
            "FLH_ely_opt": np.nan,
            "m_H2_year": np.nan,
            "K_EE_ann": np.nan,
            "K_ELY_ann": np.nan,
            "E_el_year": np.nan,
            "n_starts": np.nan,
            "success": False,
            "message": "No feasible solution found in grid search",
            "C_pv_opt": np.nan,
            "C_wind_opt": np.nan,
            "E_pv_year": np.nan,
            "E_wind_year": np.nan,
        }
    
    # Optimale Parameter aus dreistufiger Suche
    alpha_opt = search_results["alpha_final"]
    k_opt = search_results["k_final"]
    opt_success = search_results["optimize_success"]
    
    # Detaillierte Nachricht über Optimierungsverlauf
    message_parts = [
        f"Coarse: α={search_results['alpha_coarse']:.2f}, k={search_results['k_coarse']:.2f}, LCOH={search_results['lcoh_coarse']:.2f}",
        f"Fine: α={search_results['alpha_fine']:.2f}, k={search_results['k_fine']:.2f}, LCOH={search_results['lcoh_fine']:.2f}",
        f"Final: {search_results['optimize_message']}"
    ]
    
    if search_results["improvement_coarse_to_fine"] > 0.1:
        message_parts.append(f"Coarse→Fine: {search_results['improvement_coarse_to_fine']:.1f}% better")
    
    if search_results["improvement_fine_to_final"] > 0.1:
        message_parts.append(f"Fine→Final: {search_results['improvement_fine_to_final']:.1f}% better")
    
    message = " | ".join(message_parts)
    
    # Finale Simulation mit optimalen Parametern
    results = simulate_pem_electrolyzer(pv_norm, wind_norm, alpha_opt, k_opt, dt_hours)
    
    # Kosten berechnen
    K_EE_ann = compute_annual_costs_EE(
        C_pv=results["C_pv"],
        C_wind=results["C_wind"],
        capex_pv=country_params["CAPEX_PV"],
        opex_pv_fix=country_params["OPEX_PV_FIX"],
        wacc_pv=country_params["WACC_PV"],
        lifetime_pv=country_params["LIFETIME_PV"],
        capex_wind=country_params["CAPEX_WIND"],
        opex_wind_fix=country_params["OPEX_WIND_FIX"],
        wacc_wind=country_params["WACC_WIND"],
        lifetime_wind=country_params["LIFETIME_WIND"],
    )
    
    K_ELY_ann = compute_annual_costs_ely(results["P_ely_nom"])
    K_tot_ann = K_EE_ann + K_ELY_ann
    
    LCOH = search_results["lcoh_final"]
    
    return {
        "alpha_opt_cost": alpha_opt,
        "k_opt_cost": k_opt,
        "LCOH_min": LCOH,
        "FLH_ely_opt": results["FLH_ely"],
        "m_H2_year": results["m_H2_year"],
        "K_EE_ann": K_EE_ann,
        "K_ELY_ann": K_ELY_ann,
        "E_el_year": results["E_el_year"],
        "n_starts": results["n_starts"],
        "success": opt_success,
        "message": message,
        # Zusätzliche Kennzahlen
        "C_pv_opt": results["C_pv"],
        "C_wind_opt": results["C_wind"],
        "E_pv_year": results["E_pv_year"],
        "E_wind_year": results["E_wind_year"],
    }


# ---------- Wrapper: ALLE Standorte ----------

def analyze_site_h2_costs_optimizer(site_name: str, country_params: dict, 
                                   flh_min: float = 3000.0, dt_hours: float = 1.0,
                                   counter: int = 0, total: int = 0, cache: dict = None) -> dict:
    """Führt die LCOH-Optimierung für einen Standort durch (mit PEM-Modell und Cache)."""
    start_time = time.time()
    
    # Cache-Key erstellen
    cache_key = create_cache_key(site_name, country_params, flh_min)
    
    # Prüfe Cache zuerst
    if cache and cache_key in cache:
        print_site_status(counter, total, site_name, "Cache hit - using stored result")
        cached_result = cache[cache_key].copy()
        cached_result["UWWTD Code"] = cached_result.get("UWWTD Code", "")  # Sicherstellen dass UWWTD Code da ist
        return cached_result
    
    result = {
        "alpha_opt_cost": np.nan,
        "k_opt_cost": np.nan,
        "LCOH_min": np.nan,
        "FLH_ely_opt": np.nan,
        "m_H2_year": np.nan,
        "K_EE_ann": np.nan,
        "K_ELY_ann": np.nan,
        "E_el_year": np.nan,
        "n_starts": np.nan,
        "C_pv_opt": np.nan,
        "C_wind_opt": np.nan,
        "E_pv_year": np.nan,
        "E_wind_year": np.nan,
        "opt_success": False,
        "opt_message": "No timeseries found",
    }
    
    try:
        # Status: Suche Zeitreihen
        print_site_status(counter, total, site_name, "Searching timeseries...")
        
        # Finde Zeitreihen-Dateien
        pv_csv, wind_csv = find_matching_files(site_name)
        
        if pv_csv is None or wind_csv is None:
            print_site_status(counter, total, site_name, "No timeseries found")
            return result
        
        # Status: Lade Zeitreihen
        print_site_status(counter, total, site_name, "Loading timeseries...")
        
        # Zeitreihen laden
        pv_series = read_timeseries_csv(pv_csv)
        wind_series = read_timeseries_csv(wind_csv)
        
        if pv_series.empty or wind_series.empty:
            print_site_status(counter, total, site_name, "Empty timeseries")
            return result
        
        # Auf gleiche Länge bringen
        min_len = min(len(pv_series), len(wind_series))
        pv_norm = pv_series.values[:min_len]
        wind_norm = wind_series.values[:min_len]
        
        # Status: Starte Optimierung
        print_site_status(counter, total, site_name, f"Optimizing ({min_len:,} hours)...")
        
        # LCOH-Optimierung mit PEM-Modell durchführen
        print_site_status(counter, total, site_name, "Starting optimization...")
        
        best = optimize_site_lcoh(
            pv_norm=pv_norm,
            wind_norm=wind_norm,
            country_params=country_params,
            flh_min=flh_min,
            dt_hours=dt_hours,
        )
        
        print_site_status(counter, total, site_name, f"Optimization done: success={best.get('success', False)}")
        
        result.update(best)
        result["opt_success"] = best["success"]
        result["opt_message"] = best["message"]
        
        # Debug-Ausgabe
        if counter <= 3:  # Nur für die ersten 3 Anlagen
            print(f"\nDEBUG Site {counter} ({site_name}):")
            print(f"  Success: {result['opt_success']}")
            print(f"  LCOH: {result.get('LCOH_min', 'N/A')}")
            print(f"  Alpha: {result.get('alpha_opt_cost', 'N/A')}")
            print(f"  K: {result.get('k_opt_cost', 'N/A')}")
            print(f"  Message: {result.get('opt_message', 'N/A')}")
        
        # Status: Fertig
        elapsed = time.time() - start_time
        if result["opt_success"] and result["LCOH_min"] < np.inf:
            status = f"LCOH: {result['LCOH_min']:.2f} €/kg (alpha={result['alpha_opt_cost']:.2f}, k={result['k_opt_cost']:.2f})"
        else:
            status = f"Failed: {result.get('opt_message', 'Unknown error')[:50]}"
        
        print_site_status(counter, total, site_name, status, elapsed)
        
    except Exception as e:
        elapsed = time.time() - start_time
        print_site_status(counter, total, site_name, f"Error: {str(e)[:30]}...", elapsed)
        result["opt_message"] = str(e)
    
    # Ergebnis in Cache speichern (auch bei Fehlern)
    if cache is not None:
        cache[cache_key] = result.copy()
    
    return result


def optimize_all_sites_from_database_optimizer(
    input_file: str,
    sheet_name: str,
    country_params_by_code: dict,
    flh_min: float = 3000.0,
    dt_hours: float = 1.0,
    max_sites: int = None,  # Begrenzung für Tests
) -> pd.DataFrame:
    """
    Lädt Anlagen aus einfacher Datenbank und führt H2-Kostenoptimierung mit Optimizer für alle durch.
    """
    # Excel einlesen - einfache Datenbank mit UWWTD Code, Name, Lat, Lon, Capacity
    df = pd.read_excel(input_file, sheet_name=sheet_name)
    
    print(f"Total plants in database: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    
    # Ländercode aus UWWTD Code extrahieren (erste 4 Zeichen)
    df["Country"] = df["UWWTD Code"].str[:4]
    
    # Begrenzung für Tests
    if max_sites is not None:
        df = df.head(max_sites)
        print(f"⚠ Limited to first {max_sites} plants for testing")
    
    # LCOH-Optimierung für alle Anlagen
    print(f"\n--- LCOH Optimization with PEM Electrolyzer Model ---")
    print(f"Analyzing {len(df)} plants...")
    print(f"Minimum FLH: {flh_min} h/a")
    
    # Prüfe ob Zeitreihen-Verzeichnisse existieren
    if not (PV_DIR.exists() and WIND_DIR.exists()):
        print(f"  Warning: Timeseries directories not found:")
        print(f"    PV:   {PV_DIR}")
        print(f"    Wind: {WIND_DIR}")
        print(f"  Skipping LCOH optimization.")
        return df
    
    # Cache laden
    cache = load_cache()
    
    h2_results = []
    
    print(f"\n{'='*80}")
    print(f"Starting LCOH optimization for {len(df)} plants...")
    print(f"{'='*80}")
    
    start_time_total = time.time()
    
    for counter, (idx, row) in enumerate(df.iterrows(), start=1):
        site_name = str(row["Name"]).strip()
        country = row["Country"]
        
        # Länderparameter holen
        if country not in country_params_by_code:
            # Fallback zu DE für unbekannte Länder
            country_params = country_params_by_code.get("DE", country_params_by_code[list(country_params_by_code.keys())[0]])
        else:
            country_params = country_params_by_code[country]
        
        # LCOH-Optimierung durchführen (mit Cache)
        result = analyze_site_h2_costs_optimizer(
            site_name=site_name,
            country_params=country_params,
            flh_min=flh_min,
            dt_hours=dt_hours,
            counter=counter,
            total=len(df),
            cache=cache,
        )
        result["UWWTD Code"] = row["UWWTD Code"]
        h2_results.append(result)
        
        # Zwischenergebnis alle 25 Anlagen (neue Zeile nur für Zwischenergebnisse)
        if counter % 25 == 0:
            elapsed_total = time.time() - start_time_total
            avg_time = elapsed_total / counter
            remaining_time = avg_time * (len(df) - counter)
            
            successful = sum(1 for r in h2_results if r["opt_success"])
            
            # Neue Zeile für Zwischenergebnis
            print(f"\n\nCheckpoint {counter}/{len(df)}: Success: {successful}/{counter} ({successful/counter*100:.1f}%) | "
                  f"Avg: {avg_time:.1f}s/plant | Remaining: {remaining_time/60:.1f}min | "
                  f"Total ETA: {(elapsed_total + remaining_time)/60:.1f}min")
            print("=" * 120)
    
    # Finale Zusammenfassung (neue Zeile)
    total_time = time.time() - start_time_total
    successful_final = sum(1 for r in h2_results if r["opt_success"])
    
    # Cache speichern
    save_cache(cache)
    
    print(f"\n\nFINAL RESULTS:")
    print(f"Completed: {len(df)} plants | Successful: {successful_final}/{len(df)} ({successful_final/len(df)*100:.1f}%)")
    cache_hits = len([r for r in h2_results if "Cache hit" in str(r.get("opt_message", ""))])
    print(f"Cache hits: {cache_hits}/{len(df)} ({cache_hits/len(df)*100:.1f}%)")
    print(f"Total time: {total_time/60:.1f} minutes ({total_time/3600:.1f} hours)")
    print("=" * 120)
    
    # Ergebnisse als DataFrame
    df_h2_results = pd.DataFrame(h2_results)
    
    # Merge mit ursprünglichen Daten
    df_final = df.merge(df_h2_results, on="UWWTD Code", how="left")
    
    # Statistik
    valid_count = df_final["LCOH_min"].notna().sum()
    successful_opt = df_final["opt_success"].sum()
    print(f"\n✓ LCOH optimization completed: {valid_count}/{len(df)} plants successful")
    print(f"  Optimizer success rate: {successful_opt}/{len(df)} ({successful_opt/len(df)*100:.1f}%)")
    
    if valid_count > 0:
        valid = df_final[df_final["LCOH_min"].notna()]
        print(f"\n  LCOH (€/kg H2):")
        print(f"    mean={valid['LCOH_min'].mean():.2f}, "
              f"median={valid['LCOH_min'].median():.2f}, "
              f"min={valid['LCOH_min'].min():.2f}, "
              f"max={valid['LCOH_min'].max():.2f}")
        print(f"  H2 production (kg/a per kW electrolyzer):")
        print(f"    mean={valid['m_H2_year'].mean():.1f}, "
              f"median={valid['m_H2_year'].median():.1f}, "
              f"min={valid['m_H2_year'].min():.1f}, "
              f"max={valid['m_H2_year'].max():.1f}")
        print(f"  Optimal alpha (PV share):")
        print(f"    mean={valid['alpha_opt_cost'].mean():.3f}, "
              f"median={valid['alpha_opt_cost'].median():.3f}")
        print(f"  Optimal k (oversizing):")
        print(f"    mean={valid['k_opt_cost'].mean():.2f}, "
              f"median={valid['k_opt_cost'].median():.2f}")
        print(f"  Electrolyzer starts per year:")
        print(f"    mean={valid['n_starts'].mean():.0f}, "
              f"median={valid['n_starts'].median():.0f}")
    
    return df_final


# ---------- Main Funktion ----------

def main():
    # Länderspezifische Parameter aus Excel laden
    print("Loading country-specific CAPEX/OPEX/WACC parameters...")
    country_params_by_code = load_country_parameters()
    
    # PEM-Elektrolyseur Parameter sind fest im Modell:
    # CAPEX_ELY = 1100 €/kW, OPEX_ELY_FIX = 30 €/kW·a
    # WACC_ELY = 6.5%, LIFETIME_ELY = 15 Jahre
    
    # LCOH-Optimierung für alle Anlagen durchführen (mit PEM-Modell)
    # Zweistufige Optimierung: Grob → Fein
    df_results = optimize_all_sites_from_database_optimizer(
        input_file=INPUT_FILE,
        sheet_name=SHEET_NAME,
        country_params_by_code=country_params_by_code,
        flh_min=3000.0,  # Mindest-Vollaststunden
        dt_hours=1.0,
        max_sites=None,  # Alle Anlagen
    )
    
    # Ergebnisse als zusätzliche Spalten in das H2 Renewables Sheet hinzufügen
    print(f"\nAdding LCOH optimization results as additional columns to H2 Renewables sheet in: {INPUT_FILE}")
    
    # LCOH-Optimierungsspalten mit ursprünglichen Namen
    lcoh_optimization_columns = {
        "alpha_opt_cost": "alpha [-]",
        "k_opt_cost": "k [-]", 
        "LCOH_min": "LCOH [€/kg]",
        "FLH_ely_opt": "FLH_Ely [h/a]",
        "m_H2_year": "m_H2_year [kg/a]",
        "n_starts": "n_starts [1/a]"
    }
    
    # Nur die LCOH-Optimierungsspalten für das Hinzufügen auswählen
    lcoh_columns_to_add = ["UWWTD Code"] + list(lcoh_optimization_columns.keys())
    available_lcoh_columns = [col for col in lcoh_columns_to_add if col in df_results.columns]
    
    # LCOH-Optimierungsergebnisse extrahieren und umbenennen
    lcoh_results_for_merge = df_results[available_lcoh_columns].copy()
    
    # Spalten umbenennen (außer UWWTD Code)
    rename_dict = {k: v for k, v in lcoh_optimization_columns.items() if k in lcoh_results_for_merge.columns}
    lcoh_results_for_merge = lcoh_results_for_merge.rename(columns=rename_dict)
    
    print(f"  Adding LCOH optimization columns: {list(rename_dict.values())}")
    
    # Bestehende Excel-Datei laden und H2 Renewables Sheet aktualisieren
    try:
        # Alle Sheets laden
        with pd.ExcelFile(INPUT_FILE) as xls:
            sheets_dict = {}
            for sheet_name in xls.sheet_names:
                sheets_dict[sheet_name] = pd.read_excel(xls, sheet_name=sheet_name)
        
        # H2 Renewables Sheet aktualisieren oder erstellen
        if "H2 Renewables" in sheets_dict:
            # Bestehende Daten laden
            h2_renewables = sheets_dict["H2 Renewables"]
            
            print(f"  H2 Renewables existing columns: {list(h2_renewables.columns)}")
            print(f"  H2 Renewables rows: {len(h2_renewables)}")
            
            # Keine alten Spalten entfernen - nur hinzufügen/überschreiben
            
            # Bestimme Merge-Key
            merge_key = None
            if "UWWTD Code" in h2_renewables.columns and "UWWTD Code" in lcoh_results_for_merge.columns:
                merge_key = "UWWTD Code"
            elif "Name" in h2_renewables.columns:
                # Füge Name zu LCOH results hinzu falls nicht vorhanden
                if "Name" not in lcoh_results_for_merge.columns:
                    name_mapping = df_results[["UWWTD Code", "Name"]].drop_duplicates()
                    lcoh_results_for_merge = lcoh_results_for_merge.merge(name_mapping, on="UWWTD Code", how="left")
                merge_key = "Name"
            
            if merge_key:
                print(f"  Using merge key: {merge_key}")
                # LCOH-Optimierungsspalten als zusätzliche Spalten hinzufügen
                h2_renewables_updated = h2_renewables.merge(lcoh_results_for_merge, on=merge_key, how="left")
                print(f"  After merge: {len(h2_renewables_updated)} rows, {len(h2_renewables_updated.columns)} columns")
            else:
                print(f"  Warning: No suitable merge key found, using index-based merge")
                h2_renewables_updated = h2_renewables.copy()
                # Index-basiertes Hinzufügen (nur wenn gleiche Anzahl Zeilen)
                if len(h2_renewables) == len(lcoh_results_for_merge):
                    for col in lcoh_results_for_merge.columns:
                        if col != "UWWTD Code":  # UWWTD Code nicht überschreiben
                            h2_renewables_updated[col] = lcoh_results_for_merge[col].values
                else:
                    print(f"  Error: Row count mismatch - cannot merge safely")
        else:
            print("  Creating new H2 Renewables sheet")
            # Erstelle neues Sheet mit Basis-Spalten + LCOH-Optimierung
            base_columns = ["UWWTD Code", "Name"]
            if "Name" not in lcoh_results_for_merge.columns:
                name_mapping = df_results[["UWWTD Code", "Name"]].drop_duplicates()
                lcoh_results_for_merge = lcoh_results_for_merge.merge(name_mapping, on="UWWTD Code", how="left")
            
            h2_renewables_updated = lcoh_results_for_merge
        
        # Aktualisiertes Sheet in Dictionary einfügen
        sheets_dict["H2 Renewables"] = h2_renewables_updated
        
        # Alle Sheets zurück in Excel schreiben
        with pd.ExcelWriter(INPUT_FILE, engine="openpyxl") as writer:
            for sheet_name, sheet_data in sheets_dict.items():
                sheet_data.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Prüfe Erfolg der LCOH-Optimierung
        lcoh_col = "LCOH [€/kg]"
        if lcoh_col in h2_renewables_updated.columns:
            valid_results = h2_renewables_updated[h2_renewables_updated[lcoh_col].notna()]
            if len(valid_results) > 0:
                print(f"✓ LCOH optimization results added to H2 Renewables sheet")
                print(f"  - Successful analyses: {len(valid_results)}")
                print(f"  - Best LCOH: {valid_results[lcoh_col].min():.2f} €/kg H2")
                if "m_H2_year [kg/a]" in valid_results.columns:
                    print(f"  - Best H2 production: {valid_results['m_H2_year [kg/a]'].max():.1f} kg/a per kW electrolyzer")
            else:
                print("⚠ No successful LCOH analyses found")
        
        print(f"  - Total columns in H2 Renewables: {len(h2_renewables_updated.columns)}")
        print(f"  - Total rows: {len(h2_renewables_updated)}")
            
    except Exception as e:
        print(f"Error updating H2 Renewables sheet: {e}")
        print(f"Saving as separate file instead: {OUTPUT_FILE}")
        df_results.to_excel(OUTPUT_FILE, index=False)
    
    print(f"\nDone!")


def print_efficiency_consistency_check():
    """
    Konsistenzprüfung der LHV-basierten Effizienz- und Stromverbrauchsberechnung.
    Zeigt Werte bei verschiedenen Lastpunkten zur Verifikation der neuen PCHIP-Kurve.
    """
    print("\n" + "="*80)
    print("KONSISTENZPRÜFUNG: LHV-basierte PEM-System Effizienz (PCHIP-Interpolation)")
    print("="*80)
    print(f"LHV H₂: {LHV_H2:.2f} kWh/kg")
    
    # Neue Stützpunkte anzeigen
    print(f"\nPEM-System Effizienz-Stützpunkte (LHV-basiert):")
    for i, (load, eta) in enumerate(zip(X_ETA, Y_ETA)):
        e_spec = LHV_H2 / eta
        print(f"  Load {load:.2f}: η = {eta:.3f} ({eta*100:.1f}%) → e_spec = {e_spec:.1f} kWh/kg")
    
    print(f"\nInterpolierte Werte bei Testpunkten:")
    print(f"{'Load':<6} {'η_abs':<8} {'η_%':<8} {'e_spec':<10} {'H₂/kWh':<10} {'vs.Voll':<8}")
    print("-" * 58)
    
    # Erweiterte Testpunkte inkl. der geforderten [0.1, 0.2, 0.3, 0.4, 1.0]
    test_loads = [0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.5, 0.7, 1.0]
    eta_volllast = eta_from_load(1.0)  # Referenz für Vergleich
    
    for load in test_loads:
        eta_abs = eta_from_load(load)
        eta_percent = eta_abs * 100  # Prozent
        e_spec = e_spec_from_eta(eta_abs)
        h2_per_kwh = 1.0 / e_spec  # kg H₂ pro kWh
        vs_volllast = (eta_abs / eta_volllast - 1) * 100  # Prozentuale Abweichung
        
        print(f"{load:<6.2f} {eta_abs:<8.4f} {eta_percent:<8.1f} {e_spec:<10.1f} {h2_per_kwh:<10.4f} {vs_volllast:+7.1f}%")
    
    print(f"\nPlausibilitätsprüfung:")
    eta_max = max([eta_from_load(load) for load in test_loads])
    eta_min = min([eta_from_load(load) for load in test_loads])
    load_at_max = test_loads[np.argmax([eta_from_load(load) for load in test_loads])]
    
    print(f"  Max. Effizienz: {eta_max:.4f} ({eta_max*100:.1f}%) bei Load {load_at_max:.2f}")
    print(f"  Min. Effizienz: {eta_min:.4f} ({eta_min*100:.1f}%) bei Load {test_loads[0]:.2f}")
    print(f"  Effizienz-Spanne: {(eta_max-eta_min)*100:.1f} Prozentpunkte")
    
    # Interpolationsmethode anzeigen
    try:
        from scipy.interpolate import PchipInterpolator
        interp_method = "SciPy PchipInterpolator"
    except ImportError:
        interp_method = "NumPy-basierte PCHIP (Fritsch-Carlson)"
    
    print(f"  Interpolationsmethode: {interp_method}")
    
    if eta_max > 0.95:
        print("  ⚠ WARNUNG: Maximale Effizienz > 95% - physikalisch unrealistisch!")
    if eta_min < 0.05:
        print("  ⚠ WARNUNG: Minimale Effizienz < 5% - sehr ineffizient!")
    
    print(f"\nHinweise:")
    print(f"  • η_abs ist die absolute LHV-Effizienz (niemals > 1.0)")
    print(f"  • 'vs.Voll' zeigt Abweichung zur Volllast-Effizienz ({eta_volllast:.1%})")
    print(f"  • Teillast-Optimum bei Load 0.30-0.40 ist physikalisch normal")
    print("="*80)


if __name__ == "__main__":
    # 1. Lookup-Tabelle für ultra-schnelle Effizienz-Berechnung erstellen
    print("Initializing efficiency lookup table...")
    _create_eta_lookup_table()
    
    # 2. Konsistenzprüfung der Effizienz-Kurve
    print_efficiency_consistency_check()
    
    # 3. Hauptberechnung (jetzt mit ultra-schneller η-Berechnung)
    main()
