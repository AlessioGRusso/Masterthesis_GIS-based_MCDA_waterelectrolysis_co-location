
import os, re, json, time, hashlib, sys
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List, Iterable

import requests
import pandas as pd
from collections import deque

# Konfiguration

API_TOKEN = "25e16250c48705a5aeb3e4dbd81297030e088cdc"

# Zeitraum
DATE_FROM = "2015-01-01"
DATE_TO   = "2024-12-31"

# Rate Limiting
REQUESTS_PER_SECOND = 1.0
REQUESTS_PER_HOUR   = 50
AUTO_WAIT_AFTER_LIMIT = True
WAIT_AFTER_LIMIT_MINUTES = 61

# API
DATASET = "merra2"
USE_LOCAL_TIME = False

# PV-Einstellungen
PV_DEFAULTS = {
    "capacity": 1.0,
    "system_loss": 0.10,
    "tracking": 0,
    "tilt": 35,
    "azim": 180,
    "format": "csv",
    "local_time": USE_LOCAL_TIME,
    "header": "true",
}

# Wind-Einstellungen
WIND_DEFAULTS = {
    "capacity": 1.0,
    "height": 120,
    "turbine": "Vestas V112 3000",
    "format": "csv",
    "local_time": USE_LOCAL_TIME,
    "header": "true",
}

# Optional: Rohwetter-Spalten mitliefern?
INCLUDE_RAW = False

AUTO_PV_TILT_BY_LAT = True

# Watchdog/Timeouts
CONNECT_TIMEOUT_S = 30
READ_TIMEOUT_S    = 90
MAX_429_RETRIES   = 3
HOURLY_SLEEP_NOTICE_THRESHOLD_S = 10
HOURLY_SLEEP_TICK_S = 60

# Pfade
ROOT = Path(__file__).resolve().parent
EXCEL_PATH = ROOT / "Output" / "UWWTD_TP_Database.xlsx"
SHEET_GENERAL = "General Data"
SHEET_ENERGY  = "Grid Energy Connection"

OUT_BASE   = ROOT / "Output" / "RenewableEnergyProfiles"
OUT_PV     = OUT_BASE / "PV"
OUT_WIND   = OUT_BASE / "Wind"
PROGRESS_PATH = OUT_BASE / "progress.json"

API_BASE = "https://www.renewables.ninja/api"
PV_ENDPOINT = f"{API_BASE}/data/pv"
WIND_ENDPOINT = f"{API_BASE}/data/wind"

RUN_STAMP = datetime.now().strftime("%Y%m%d")
LOG_FILE = OUT_BASE / f"run_{RUN_STAMP}.log"
SUMMARY_FILE = OUT_BASE / f"run_summary_{RUN_STAMP}.csv"  # nur intern/log; keine KPI-CSV

# ===== HELFER =================================================================

class FatalError(Exception):
    pass

def dparse(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def year_chunks(dfrom: date, dto: date) -> Iterable[Tuple[date, date]]:
    cur = dfrom
    while cur <= dto:
        next_year_start = date(cur.year + 1, 1, 1)
        chunk_end = min(dto, next_year_start - timedelta(days=1))
        yield (cur, chunk_end)
        cur = next_year_start

def compute_tilt_from_lat(latitude_deg: float) -> float:
    t = latitude_deg - 10.0
    if t < 20.0: t = 20.0
    if t > 45.0: t = 45.0
    return float(t)

def ensure_token() -> str:
    token = (API_TOKEN or "").strip()
    if not token or token == "DEIN_TOKEN_HIER":
        env = (os.getenv("RENEWABLES_NINJA_TOKEN") or "").strip()
        if env: token = env
    if not token or token == "DEIN_TOKEN_HIER":
        raise FatalError("Kein API-Token gesetzt. Trage ihn oben ein oder setze RENEWABLES_NINJA_TOKEN.")
    return token

def log(msg: str) -> None:
    # Print mit error='replace' für Windows-Konsole
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        print(msg.encode('ascii', errors='replace').decode('ascii'), flush=True)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat(timespec='seconds')} {msg}\n")

def sanitize_name(name: str) -> str:
    n = re.sub(r"[^\w\-]+", "_", str(name).strip())
    n = re.sub(r"_+", "_", n).strip("_")
    return n[:120] or "site"

def short_hash(*parts) -> str:
    return hashlib.sha1("||".join(map(str, parts)).encode("utf-8")).hexdigest()[:10]

def find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    lc = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        if cand.lower().strip() in lc:
            return lc[cand.lower().strip()]
    for c in df.columns:
        if any(k.lower() in c.lower() for k in candidates):
            return c
    return None

def pick_name_column(df: pd.DataFrame) -> Optional[str]:
    return find_column(df, ["Name","UWWTP Name","WWTP Name","Plant","Plant Name","TP Name",
                            "UWWTD Name","Facility","Site","Anlagenname","Station","Station Name"])

def pick_coord_columns(df: pd.DataFrame) -> Tuple[str, str]:
    lat = find_column(df, ["Latitude","Lat","Lat.","Breite"]) or next((c for c in df.columns if c.lower()=="latitude"), None)
    lon = find_column(df, ["Longitude","Long","Lon","Long.","Länge","Lng"]) or next((c for c in df.columns if c.lower()=="longitude"), None)
    if not lat or not lon:
        raise FatalError("Konnte 'Latitude'/'Longitude' in Sheet 'General Data' nicht finden.")
    return lat, lon

def load_progress() -> Dict[str, Any]:
    if PROGRESS_PATH.is_file():
        try: return json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
        except Exception: return {}
    return {}

def save_progress(progress: Dict[str, Any]) -> None:
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = PROGRESS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(progress, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(PROGRESS_PATH)

def sleep_with_countdown(seconds: float, prefix: str) -> None:
    remaining = int(seconds)
    resume_at = (datetime.now() + timedelta(seconds=remaining)).strftime("%H:%M:%S")
    log(f"{prefix} – warte {remaining//60}m{remaining%60}s (nächster Slot: {resume_at})")
    while remaining > 0:
        tick = HOURLY_SLEEP_TICK_S if remaining > HOURLY_SLEEP_TICK_S else remaining
        time.sleep(tick)
        remaining -= tick
        if remaining > 0:
            log(f"... noch {remaining//60}m{remaining%60}s")

class RateLimiter:
    """Doppel-Limiter: 1 req/s UND 50 req/h, mit sichtbarer Warteanzeige."""
    def __init__(self, per_second: float, per_hour: int):
        if per_second <= 0: raise FatalError("per_second muss > 0 sein.")
        if per_hour   <= 0: raise FatalError("per_hour muss > 0 sein.")
        self.interval = 1.0 / per_second
        self.per_hour = per_hour
        self.last_ts = 0.0
        self.last_hour = deque()
        self.request_count = 0

    def wait(self):
        now = time.time()
        # 1) pro Sekunde
        sleep_s = (self.last_ts + self.interval) - now
        if sleep_s > 0:
            time.sleep(sleep_s)
            now = time.time()
        self.last_ts = now
        # 2) pro Stunde - alte Einträge entfernen
        while self.last_hour and now - self.last_hour[0] >= 3600:
            self.last_hour.popleft()
        
        # Limit erreicht?
        if len(self.last_hour) >= self.per_hour:
            if AUTO_WAIT_AFTER_LIMIT:
                # Nach 50 Requests: Script beenden, damit Loop-Script neu starten kann
                log(f"Hourly-Limit {self.per_hour}/h erreicht nach {self.request_count} Requests.")
                log("Script beendet sich – Loop-Script startet in 61 Min neu.")
                sys.exit(2)  # Exit-Code 2 = Limit erreicht, nicht fertig (Progress wird in fetch_merge_timeseries gesichert)
            else:
                # Manuelles Warten (alte Logik)
                wait_s = 3600 - (now - self.last_hour[0]) + 0.1
                if wait_s >= HOURLY_SLEEP_NOTICE_THRESHOLD_S:
                    sleep_with_countdown(wait_s, prefix=f"Hourly-Limit {self.per_hour}/h erreicht")
                else:
                    time.sleep(wait_s)
                now = time.time()
                while self.last_hour and now - self.last_hour[0] >= 3600:
                    self.last_hour.popleft()
        
        self.last_hour.append(time.time())
        self.request_count += 1

def clean_params(p: Dict[str, Any]) -> Dict[str, Any]:
    q = {}
    for k, v in p.items():
        if v is None: continue
        if isinstance(v, str) and v == "": continue
        if isinstance(v, bool):
            q[k] = "true" if v else "false"
        else:
            q[k] = v
    return q

def fetch_chunk_text(session: requests.Session, url: str, params: Dict[str, Any],
                     limiter: RateLimiter) -> str:
    retries_429 = 0
    backoff = 5
    while True:
        limiter.wait()
        try:
            r = session.get(
                url, params=clean_params(params),
                timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S)
            )
        except requests.RequestException as e:
            raise FatalError(f"Request-Fehler bei {url.split('/')[-1]}: {e}")

        code = r.status_code
        if code == 200:
            return r.text
        if code == 429:
            ra = r.headers.get("Retry-After")
            try: wait_s = int(float(ra)) if ra else backoff
            except ValueError: wait_s = backoff
            log(f"  !! 429 Too Many Requests – warte {wait_s}s (Retry-After={ra})")
            time.sleep(wait_s); retries_429 += 1
            if retries_429 > MAX_429_RETRIES:
                raise FatalError("Abbruch: wiederholt 429 Too Many Requests.")
            backoff = min(backoff * 2, 900); continue
        head = r.text[:200].replace("\n", " ")
        if 500 <= code < 600: raise FatalError(f"Serverfehler {code} bei {url.split('/')[-1]} – {head}")
        if 400 <= code < 500: raise FatalError(f"Client-/Auth-Fehler {code} bei {url.split('/')[-1]} – {head}")
        raise FatalError(f"Unerwarteter Fehler {code} – {head}")

def append_csv_chunk(part_path: Path, csv_text: str, *, first_chunk: bool) -> None:
    lines = csv_text.splitlines()
    data = [ln for ln in lines if not ln.startswith("#")]
    if not data: return
    rows = data if first_chunk else data[1:]  # Header nur einmal
    mode = "w" if first_chunk else "a"
    part_path.parent.mkdir(parents=True, exist_ok=True)
    with part_path.open(mode, encoding="utf-8", newline="\n") as f:
        f.write("\n".join(rows)); f.write("\n")

def expected_rows_for_range(d0: date, d1: date) -> int:
    total = 0
    for y in range(d0.year, d1.year + 1):
        leap = (y % 400 == 0) or (y % 4 == 0 and y % 100 != 0)
        total += 8784 if leap else 8760
    return total

def read_csv_strip_comments(path: Path) -> pd.DataFrame:
    lines = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            if ln.startswith("#"): continue
            lines.append(ln)
    if not lines:
        return pd.DataFrame()
    tmp = OUT_BASE / "_tmp_read.csv"
    tmp.write_text("".join(lines), encoding="utf-8")
    try:
        df = pd.read_csv(tmp)
    finally:
        try: tmp.unlink()
        except Exception: pass
    return df

def validate_complete_csv(path: Path, d0: date, d1: date) -> bool:
    if (not path.exists()) or path.suffix.endswith(".part"): return False
    try:
        df = read_csv_strip_comments(path)
    except Exception:
        return False
    if "time" not in df.columns or "electricity" not in df.columns: return False
    try:
        t = pd.to_datetime(df["time"], utc=True, errors="raise")
    except Exception:
        return False
    if not t.is_monotonic_increasing: return False
    if t.duplicated().any(): return False
    if df["electricity"].isna().any(): return False
    # Wertebereich
    if (df["electricity"] > 1.05).sum() > 0: return False
    if (df["electricity"] < -1e-6).sum() > 0: return False
    # Rowcount (tolerieren ±12h, falls Rand inclusive/exclusive minimal differiert)
    exp = expected_rows_for_range(d0, d1)
    if abs(len(df) - exp) > 12: return False
    return True

def build_signature(tech: str, params: Dict[str, Any]) -> str:
    # Nur relevante Parameter für Stabilität in die Signatur
    if tech.lower() == "pv":
        items = {
            "dataset": DATASET,
            "local_time": params.get("local_time", False),
            "capacity": params.get("capacity", 1.0),
            "system_loss": params.get("system_loss"),
            "tracking": params.get("tracking"),
            "azim": params.get("azim"),
            "tilt": round(float(params.get("tilt", 0.0)), 1),
        }
    else:
        items = {
            "dataset": DATASET,
            "local_time": params.get("local_time", False),
            "capacity": params.get("capacity", 1.0),
            "height": params.get("height"),
            "turbine": params.get("turbine"),
        }
    key = json.dumps(items, sort_keys=True, ensure_ascii=False)
    return short_hash(key)

def target_filename(tech: str, plant_name: str, params: Dict[str, Any]) -> str:
    daterange = f"{DATE_FROM.replace('-','')}-{DATE_TO.replace('-','')}"
    sig = build_signature(tech, params)
    return f"{tech}_{plant_name}_{daterange}_{DATASET}_{sig}.csv"

def fetch_merge_timeseries(session: requests.Session, endpoint: str, base_params: Dict[str, Any],
                           final_path: Path, limiter: RateLimiter,
                           progress: Dict[str, Any], job_key: str) -> str:
    d0, d1 = dparse(base_params["date_from"]), dparse(base_params["date_to"])
    chunks = list(year_chunks(d0, d1))
    total_chunks = len(chunks)
    part_path = final_path.with_suffix(final_path.suffix + ".part")

    p = progress.get(job_key, {})
    done_chunks = int(p.get("done_chunks", 0)) if isinstance(p.get("done_chunks", 0), (int, float)) else 0
    if (not part_path.exists()) and done_chunks > 0 and p.get("status") != "done":
        done_chunks = 0
    if part_path.exists() and done_chunks == 0 and p.get("status") != "done":
        try: part_path.unlink()
        except Exception: pass

    for idx, (c_from, c_to) in enumerate(chunks, start=1):
        if idx <= done_chunks: continue
        params = dict(base_params)
        params.update({"date_from": c_from.isoformat(), "date_to": c_to.isoformat()})
        log(f"  -> Chunk {idx}/{total_chunks}: {params['date_from']}..{params['date_to']}")
        text = fetch_chunk_text(session, endpoint, params, limiter)
        append_csv_chunk(part_path, text, first_chunk=(idx == 1 and done_chunks == 0))
        progress[job_key] = {
            "status": "partial", "path_part": str(part_path),
            "done_chunks": idx, "total_chunks": total_chunks,
        }
        save_progress(progress)

    part_path.replace(final_path)
    progress[job_key] = {"status": "done", "path": str(final_path), "chunks": total_chunks}
    save_progress(progress)
    return "OK"

# ===== KPI-BERECHNUNG =========================================================



def compute_kpis_for_site(pv_csv: Path, wind_csv: Path) -> Dict[str, Any]:
    pv = read_csv_strip_comments(pv_csv)
    wd = read_csv_strip_comments(wind_csv)
    # Parse + align
    pv["time"] = pd.to_datetime(pv["time"], utc=True)
    wd["time"] = pd.to_datetime(wd["time"], utc=True)
    pv = pv[["time", "electricity"]].rename(columns={"electricity":"pv"})
    wd = wd[["time", "electricity"]].rename(columns={"electricity":"wind"})
    df = pd.merge(pv, wd, on="time", how="inner").sort_values("time").reset_index(drop=True)

    # 1. FLH_PV_h: Jahres-FLH (Summe electricity) und Mittel über Jahre
    pv_year = df.set_index("time")["pv"].groupby(pd.Grouper(freq="YE")).sum()
    flh_pv = float(pv_year.mean()) if len(pv_year) else float("nan")

    # 2. FLH_Wind_h: Jahres-FLH (Summe electricity) und Mittel über Jahre
    wd_year = df.set_index("time")["wind"].groupby(pd.Grouper(freq="YE")).sum()
    flh_wd = float(wd_year.mean()) if len(wd_year) else float("nan")

    # FLH_RES_mix_h entfernt - wird nicht mehr berechnet

    # Dark Calm Share entfernt - wird nicht mehr berechnet

    return {
        "FLH_PV_h": round(flh_pv, 2),
        "FLH_Wind_h": round(flh_wd, 2),
    }

def write_kpis_to_excel(excel_path: Path, sheet_general: str, sheet_energy: str,
                        kpi_rows: List[Dict[str, Any]]) -> None:
    # Basis: Namen + Koordinaten aus "General Data"
    df_gen = pd.read_excel(excel_path, sheet_name=sheet_general)
    name_col = pick_name_column(df_gen)
    lat_col, lon_col = pick_coord_columns(df_gen)
    base = df_gen[[c for c in [name_col, lat_col, lon_col] if c]].copy()
    base.columns = ["Name", "Latitude", "Longitude"]

    kpi_df = pd.DataFrame(kpi_rows)
    # Merge: Basis links, KPIs rechts (Namensschlüssel)
    merged = base.merge(kpi_df, on="Name", how="left")

    # Falls Sheet 'Grid Energy Connection' schon existiert: alte Spalten beibehalten, KPI-Spalten aktualisieren
    try:
        df_energy_old = pd.read_excel(excel_path, sheet_name=sheet_energy)
        if "Name" in df_energy_old.columns:
            # Join auf Name, KPI-Spalten überschreiben/ergänzen
            df_energy = df_energy_old.set_index("Name")
            df_new = merged.set_index("Name")
            for col in ["Latitude","Longitude","FLH_PV_h","FLH_Wind_h"]:
                df_energy[col] = df_new[col]
            df_energy = df_energy.reset_index()
        else:
            # kein Name in bestehendem Sheet -> ersetzen durch merged
            df_energy = merged
    except Exception:
        # Sheet existiert nicht: neu anlegen
        df_energy = merged

    # Spaltenreihenfolge nett
    lead = ["Name","Latitude","Longitude","FLH_PV_h","FLH_Wind_h"]
    other = [c for c in df_energy.columns if c not in lead]
    df_out = df_energy[lead + other]

    # Zurückschreiben (andere Sheets behalten)
    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as xw:
        df_out.to_excel(xw, sheet_name=sheet_energy, index=False)

    log(f"KPI-Update in Excel geschrieben: {excel_path} -> Sheet '{sheet_energy}'")


def write_h2_renewables_base(excel_path: Path, sheet_general: str, sheet_h2: str,
                              kpi_rows: List[Dict[str, Any]]) -> None:
    """
    Schreibt nur FLH_PV_h und FLH_Wind_h ins H2 Renewables Sheet.
    FLH_RES_mix_h und DarkShare_10pct werden nicht mehr geschrieben.
    Dieses Sheet wird später vom Elektrolyse-Skript erweitert.
    """
    # Basis: UWWTD Code und Namen aus "General Data"
    df_gen = pd.read_excel(excel_path, sheet_name=sheet_general)
    name_col = pick_name_column(df_gen)
    
    # UWWTD Code und Name in richtiger Reihenfolge
    if "UWWTD Code" in df_gen.columns:
        base = df_gen[["UWWTD Code", name_col]].copy()
        base.columns = ["UWWTD Code", "Name"]
    else:
        base = df_gen[[name_col]].copy()
        base.columns = ["Name"]

    kpi_df = pd.DataFrame(kpi_rows)
    # Merge: Basis links, KPIs rechts (Namensschlüssel)
    merged = base.merge(kpi_df, on="Name", how="left")

    # Falls Sheet 'H2 Renewables' schon existiert: alte Spalten beibehalten, FLH-Spalten aktualisieren
    try:
        df_h2_old = pd.read_excel(excel_path, sheet_name=sheet_h2)
        
        # Bestimme Merge-Key (bevorzuge UWWTD Code, fallback auf Name)
        merge_key = "UWWTD Code" if "UWWTD Code" in df_h2_old.columns and "UWWTD Code" in merged.columns else "Name"
        
        if merge_key in df_h2_old.columns:
            # Join auf Merge-Key, FLH-Spalten überschreiben/ergänzen
            df_h2 = df_h2_old.set_index(merge_key)
            df_new = merged.set_index(merge_key)
            for col in ["FLH_PV_h", "FLH_Wind_h"]:
                if col in df_new.columns:
                    df_h2[col] = df_new[col]

            for col in ["UWWTD Code", "Name"]:
                if col in df_new.columns and col not in df_h2.columns:
                    df_h2[col] = df_new[col]
            df_h2 = df_h2.reset_index()
        else:

            df_h2 = merged
    except Exception:

        df_h2 = merged

    # Spaltenreihenfolge: UWWTD Code, Name, FLH_PV_h, FLH_Wind_h, dann andere
    lead = []
    if "UWWTD Code" in df_h2.columns:
        lead.append("UWWTD Code")
    if "Name" in df_h2.columns:
        lead.append("Name")
    lead.extend(["FLH_PV_h", "FLH_Wind_h"])
    
    other = [c for c in df_h2.columns if c not in lead]
    df_out = df_h2[lead + other]

    # Zurückschreiben (andere Sheets behalten)
    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as xw:
        df_out.to_excel(xw, sheet_name=sheet_h2, index=False)

    log(f"H2 Renewables Base geschrieben: {excel_path} -> Sheet '{sheet_h2}'")

# ===== HAUPTLAUF ===============================================================

def main():
    token = ensure_token()
    OUT_PV.mkdir(parents=True, exist_ok=True)
    OUT_WIND.mkdir(parents=True, exist_ok=True)

    if not EXCEL_PATH.is_file():
        raise FatalError(f"Excel-Datei nicht gefunden: {EXCEL_PATH}")

    df_sites = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_GENERAL)
    name_col = pick_name_column(df_sites)
    lat_col, lon_col = pick_coord_columns(df_sites)
    rows = df_sites[[c for c in [name_col, lat_col, lon_col] if c]].dropna(subset=[lat_col, lon_col]).reset_index(drop=True)

    progress = load_progress()
    limiter = RateLimiter(REQUESTS_PER_SECOND, REQUESTS_PER_HOUR)
    s = requests.Session(); s.headers.update({"Authorization": f"Token {token}"})

    total_sites = len(rows)
    d0, d1 = dparse(DATE_FROM), dparse(DATE_TO)

    log("======================================================")
    log(f"Start • Sites: {total_sites} • Zeitraum: {DATE_FROM} bis {DATE_TO} (Auto-Chunking <=1Y)")
    log(f"Limiter: {REQUESTS_PER_SECOND} req/s & {REQUESTS_PER_HOUR} req/h (sichtbar)")
    log(f"Excel: {EXCEL_PATH} | Sheets: '{SHEET_GENERAL}' -> '{SHEET_ENERGY}'")
    log(f"Ausgabe: {OUT_BASE} (stabile Dateinamen, keine Doppel-Downloads)")
    log("======================================================")

    # --------- PHASE A: Inventur + ggf. Download + KPI-Berechnung ---------
    summary_rows = []
    kpi_rows = []
    
    for i, row in rows.iterrows():
        raw_name = row[name_col] if name_col else f"site_{i+1}"
        plant_name = sanitize_name(raw_name)
        original_name = str(raw_name).strip()  # Original-Name für Excel-Merge
        lat = float(row[lat_col]); lon = float(row[lon_col])

        # PV params
        pv_params = dict(PV_DEFAULTS)
        pv_params.update({"lat": lat, "lon": lon, "date_from": DATE_FROM, "date_to": DATE_TO, "dataset": DATASET})
        if AUTO_PV_TILT_BY_LAT: pv_params["tilt"] = compute_tilt_from_lat(lat)
        if INCLUDE_RAW: pv_params["raw"] = "true"
        pv_file = OUT_PV / target_filename("PV", plant_name, pv_params)
        pv_job  = f"{plant_name}::pv::{DATE_FROM}::{DATE_TO}::{build_signature('PV', pv_params)}"

        # Wind params
        w_params = dict(WIND_DEFAULTS)
        w_params.update({"lat": lat, "lon": lon, "date_from": DATE_FROM, "date_to": DATE_TO, "dataset": DATASET})
        if INCLUDE_RAW: w_params["raw"] = "true"
        w_file = OUT_WIND / target_filename("Wind", plant_name, w_params)
        w_job  = f"{plant_name}::wind::{DATE_FROM}::{DATE_TO}::{build_signature('Wind', w_params)}"

        # Status prüfen
        pv_complete = validate_complete_csv(pv_file, d0, d1)
        w_complete  = validate_complete_csv(w_file, d0, d1)

        # Download nur wenn nötig
        try:
            if not pv_complete:
                log(f"[{i+1}/{total_sites}] {plant_name} PV fehlt/inkomplett -> lade … ({pv_file.name})")
                fetch_merge_timeseries(s, PV_ENDPOINT, pv_params, pv_file, limiter, progress, pv_job)
                pv_complete = validate_complete_csv(pv_file, d0, d1)
                if not pv_complete:
                    raise FatalError(f"PV-Datei bleibt inkorrekt: {pv_file.name}")

            if not w_complete:
                log(f"[{i+1}/{total_sites}] {plant_name} Wind fehlt/inkomplett -> lade … ({w_file.name})")
                fetch_merge_timeseries(s, WIND_ENDPOINT, w_params, w_file, limiter, progress, w_job)
                w_complete = validate_complete_csv(w_file, d0, d1)
                if not w_complete:
                    raise FatalError(f"Wind-Datei bleibt inkorrekt: {w_file.name}")

        except FatalError as e:
            save_progress(progress)
            log("======================================================")
            log(f"FEHLER – {plant_name} ({lat:.5f},{lon:.5f}): {e}")
            log("Abbruch. Neustart setzt Downloads fort; bereits vollständige Dateien werden übersprungen.")
            log("======================================================")
            if summary_rows:
                pd.DataFrame(summary_rows).to_csv(SUMMARY_FILE, index=False, encoding="utf-8")
            sys.exit(1)

        # Inventur-Status
        pv_tag = "COMPLETE" if pv_complete else "MISSING"
        w_tag  = "COMPLETE" if w_complete else "MISSING"
        log(f"[{i+1}/{total_sites}] {plant_name} | PV:{pv_tag} | WND:{w_tag}")
        summary_rows.append({"Name": plant_name, "PV_File": pv_file.name, "WIND_File": w_file.name})
        
        # KPIs berechnen, wenn beide Dateien komplett sind
        if pv_complete and w_complete:
            try:
                kpis = compute_kpis_for_site(pv_file, w_file)
                kpi_rows.append({"Name": original_name, **kpis})
                log(f"  -> KPIs berechnet: FLH_PV={kpis['FLH_PV_h']:.1f}h, FLH_Wind={kpis['FLH_Wind_h']:.1f}h")
            except Exception as e:
                log(f"  -> WARNUNG: KPI-Berechnung fehlgeschlagen: {e}")
                kpi_rows.append({"Name": original_name, "FLH_PV_h": float("nan"), "FLH_Wind_h": float("nan")})
        else:
            # Platzhalter für fehlende Daten
            kpi_rows.append({"Name": original_name, "FLH_PV_h": float("nan"), "FLH_Wind_h": float("nan")})


    log("======================================================")
    log(f"Schreibe {len(kpi_rows)} KPI-Zeilen in Excel 'H2 Renewables' …")
    

    write_h2_renewables_base(EXCEL_PATH, SHEET_GENERAL, "H2 Renewables", kpi_rows)
    
    log(f"✓ Renewable energy KPIs calculated for {len(kpi_rows)} plants → Sheet 'H2 Renewables'")

if __name__ == "__main__":
    try:
        main()
    except FatalError as e:
        print(f"FEHLER: {e}", flush=True)
        sys.exit(1)
    except Exception as e:
        print(f"Unerwarteter Fehler: {e}", flush=True)
        sys.exit(1)

