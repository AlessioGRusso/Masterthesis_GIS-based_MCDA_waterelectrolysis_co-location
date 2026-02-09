

from pathlib import Path
import pandas as pd
import geopandas as gpd
import numpy as np
from typing import Dict, List, Tuple
import warnings
import requests
import time
from shapely.geometry import Point, box
warnings.filterwarnings('ignore')

# Konfiguration
ROOT = Path(__file__).resolve().parent
DATA = ROOT / "Daten"
OUTPUT = ROOT / "Output"

# Input
WWTP_XLSX = OUTPUT / "UWWTD_TP_Database.xlsx"
WWTP_SHAPES = OUTPUT / "WWTP Geopackages" / "WWTPS_Shapes.gpkg"
ROUTES_DIR = OUTPUT / "Geopackages"

# Abnehmer-Daten
INDUSTRY_FILE = DATA / "D5_1_Industry_Dataset.geojson"
PORTS_FILE = DATA / "ports.geojson"
AIRPORTS_FILE = DATA / "airports.geojson"

# Parameter
RADIUS_KM = 10.0
PIPELINE_BUFFER_KM = 2.0
TARGET_CRS = "EPSG:3035"
BUILT_FLAG_COL = "Built Scenario 1 (EHB)"

# Industrie-Filter (nur relevante Typen für H2-Abnehmer)
INDUSTRY_TYPES = [
    'Chemical industry',      # Chemische Industrie
    'Iron and steel',         # Stahlindustrie
    'Non-metallic minerals'   # Zement, Glas, Keramik
]
INDUSTRY_TYPE_COL = 'Eurostat_Name'  # Spalte mit Industrie-Typ

# Sheets
GENERAL_SHEET = "General Data"
PIPELINE_SHEET = "H2 Logistics"
KEY_COL = "UWWTD Code"


def log(msg: str):
    """Einfache Log-Funktion"""
    print(msg)


def fetch_osm_airports(bbox: Tuple[float, float, float, float]) -> gpd.GeoDataFrame:
    """
    Lädt Flughäfen aus OpenStreetMap via Overpass API
    bbox: (min_lon, min_lat, max_lon, max_lat)
    Nur große Flughäfen (international/regional)
    """
    log("   Lade Flughäfen von OpenStreetMap...")
    
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    # Query für Flughäfen (nur größere: international, regional)
    query = f"""
    [out:json][timeout:180];
    (
      node["aeroway"="aerodrome"]["aerodrome:type"~"international|regional"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
      way["aeroway"="aerodrome"]["aerodrome:type"~"international|regional"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
      relation["aeroway"="aerodrome"]["aerodrome:type"~"international|regional"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
    );
    out center;
    """
    
    try:
        response = requests.post(overpass_url, data=query, timeout=120)
        response.raise_for_status()
        data = response.json()
        
        # Parse Ergebnisse
        features = []
        for element in data.get('elements', []):
            if element['type'] == 'node':
                lat, lon = element['lat'], element['lon']
            elif 'center' in element:
                lat, lon = element['center']['lat'], element['center']['lon']
            else:
                continue
            
            features.append({
                'name': element.get('tags', {}).get('name', 'Unknown'),
                'type': element.get('tags', {}).get('aerodrome:type', 'unknown'),
                'iata': element.get('tags', {}).get('iata', ''),
                'geometry': Point(lon, lat)
            })
        
        if features:
            gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
            log(f"      Gefunden: {len(gdf)} Flughäfen")
            return gdf
        else:
            log(f"      Keine Flughäfen gefunden")
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
            
    except Exception as e:
        log(f"      ⚠️  Fehler beim Laden von OSM Flughäfen: {e}")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")


def fetch_osm_ports(bbox: Tuple[float, float, float, float]) -> gpd.GeoDataFrame:
    """
    Lädt Häfen aus OpenStreetMap via Overpass API
    bbox: (min_lon, min_lat, max_lon, max_lat)
    Nur größere Häfen
    """
    log("   Lade Häfen von OpenStreetMap...")
    
    overpass_url = "http://overpass-api.de/api/interpreter"
    
    # Query für Häfen (industrial, commercial)
    query = f"""
    [out:json][timeout:180];
    (
      node["harbour"="yes"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
      node["industrial"="port"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
      way["harbour"="yes"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
      way["industrial"="port"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
      way["landuse"="port"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
      relation["harbour"="yes"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
      relation["industrial"="port"]({bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]});
    );
    out center;
    """
    
    try:
        response = requests.post(overpass_url, data=query, timeout=120)
        response.raise_for_status()
        data = response.json()
        
        # Parse Ergebnisse
        features = []
        for element in data.get('elements', []):
            if element['type'] == 'node':
                lat, lon = element['lat'], element['lon']
            elif 'center' in element:
                lat, lon = element['center']['lat'], element['center']['lon']
            else:
                continue
            
            features.append({
                'name': element.get('tags', {}).get('name', 'Unknown'),
                'type': element.get('tags', {}).get('harbour', 'port'),
                'geometry': Point(lon, lat)
            })
        
        if features:
            gdf = gpd.GeoDataFrame(features, crs="EPSG:4326")
            log(f"      Gefunden: {len(gdf)} Häfen")
            return gdf
        else:
            log(f"      Keine Häfen gefunden")
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
            
    except Exception as e:
        log(f"      ⚠️  Fehler beim Laden von OSM Häfen: {e}")
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")


def load_wwtp_data() -> Tuple[pd.DataFrame, gpd.GeoDataFrame]:
    """Lädt WWTP Daten aus Excel und erstellt Geometrien aus Koordinaten"""
    log("\n1. Lade WWTP Daten...")
    
    # Excel Daten - H2 Logistics Sheet
    pipeline_df = pd.read_excel(WWTP_XLSX, sheet_name=PIPELINE_SHEET)
    log(f"   Geladen: {len(pipeline_df)} Anlagen aus '{PIPELINE_SHEET}'")
    
    # Lade General Data für Koordinaten
    general_df = pd.read_excel(WWTP_XLSX, sheet_name=GENERAL_SHEET)
    
    # Suche nach Lat/Lon Spalten in General Data
    lat_col = None
    lon_col = None
    for col in general_df.columns:
        col_lower = col.lower()
        if 'latitude' in col_lower or col_lower == 'lat':
            lat_col = col
        if 'longitude' in col_lower or col_lower in ['lon', 'long']:
            lon_col = col
    
    if not lat_col or not lon_col:
        raise ValueError(f"Keine Koordinaten-Spalten in General Data gefunden.")
    
    log(f"   Koordinaten-Spalten: {lat_col}, {lon_col}")
    
    # Merge Pipeline mit General Data für Koordinaten
    coords_df = general_df[[KEY_COL, lat_col, lon_col]]
    merged_df = pipeline_df.merge(coords_df, on=KEY_COL, how='left')
    
    # Erstelle Geometrien
    geometry = [Point(lon, lat) if pd.notna(lat) and pd.notna(lon) else None 
                for lat, lon in zip(merged_df[lat_col], merged_df[lon_col])]
    
    gdf = gpd.GeoDataFrame(merged_df, geometry=geometry, crs="EPSG:4326")
    gdf = gdf[gdf.geometry.notna()]  # Entferne Zeilen ohne Geometrie
    gdf = gdf.to_crs(TARGET_CRS)
    
    log(f"   Geometrien erstellt: {len(gdf)} Anlagen mit Koordinaten")
    
    return pipeline_df, gdf


def get_europe_bbox() -> Tuple[float, float, float, float]:
    """Gibt Bounding Box für Europa zurück (min_lon, min_lat, max_lon, max_lat)"""
    # Europa: von Portugal bis Ural, von Norwegen bis Mittelmeer
    return (-10.0, 35.0, 40.0, 71.0)


def load_offtakers(use_osm: bool = True) -> Dict[str, gpd.GeoDataFrame]:
    """Lädt alle Abnehmer-Datensätze"""
    log("\n2. Lade Abnehmer-Daten...")
    offtakers = {}
    
    # Industrie
    if INDUSTRY_FILE.exists():
        try:
            industry = gpd.read_file(INDUSTRY_FILE)
            industry = industry.to_crs(TARGET_CRS)
            # Stelle sicher, dass Geometrien gültig sind
            industry = industry[industry.geometry.notna()]
            
            # Filtere nach relevanten Industrie-Typen
            if INDUSTRY_TYPE_COL in industry.columns:
                before_filter = len(industry)
                industry = industry[industry[INDUSTRY_TYPE_COL].isin(INDUSTRY_TYPES)]
                log(f"   Industrie: {len(industry)} Standorte (gefiltert von {before_filter})")
                log(f"      Typen: {', '.join(INDUSTRY_TYPES)}")
                
                # Zeige Verteilung
                type_counts = industry[INDUSTRY_TYPE_COL].value_counts()
                for itype, count in type_counts.items():
                    log(f"      - {itype}: {count}")
            else:
                log(f"   ⚠️  Spalte '{INDUSTRY_TYPE_COL}' nicht gefunden - verwende alle Industrie")
                log(f"   Industrie: {len(industry)} Standorte")
            
            offtakers['Industry'] = industry
        except Exception as e:
            log(f"   ⚠️  Fehler beim Laden der Industrie-Datei: {e}")
    else:
        log(f"   ⚠️  Industrie-Datei nicht gefunden: {INDUSTRY_FILE.name}")
    
    # Häfen
    ports_loaded = False
    if PORTS_FILE.exists():
        try:
            ports = gpd.read_file(PORTS_FILE)
            ports = ports.to_crs(TARGET_CRS)
            ports = ports[ports.geometry.notna()]
            offtakers['Ports'] = ports
            log(f"   Häfen: {len(ports)} Standorte (aus lokaler Datei)")
            ports_loaded = True
        except Exception as e:
            log(f"   ⚠️  Fehler beim Laden der Häfen-Datei: {e}")
    
    if not ports_loaded and use_osm:
        log(f"   Häfen-Datei nicht gefunden - lade von OpenStreetMap...")
        bbox = get_europe_bbox()
        ports = fetch_osm_ports(bbox)
        if not ports.empty:
            ports = ports.to_crs(TARGET_CRS)
            offtakers['Ports'] = ports
    
    # Flughäfen
    airports_loaded = False
    if AIRPORTS_FILE.exists():
        try:
            airports = gpd.read_file(AIRPORTS_FILE)
            airports = airports.to_crs(TARGET_CRS)
            airports = airports[airports.geometry.notna()]
            offtakers['Airports'] = airports
            log(f"   Flughäfen: {len(airports)} Standorte (aus lokaler Datei)")
            airports_loaded = True
        except Exception as e:
            log(f"   ⚠️  Fehler beim Laden der Flughäfen-Datei: {e}")
    
    if not airports_loaded and use_osm:
        log(f"   Flughäfen-Datei nicht gefunden - lade von OpenStreetMap...")
        bbox = get_europe_bbox()
        airports = fetch_osm_airports(bbox)
        if not airports.empty:
            airports = airports.to_crs(TARGET_CRS)
            offtakers['Airports'] = airports
    
    if not offtakers:
        raise ValueError("Keine Abnehmer-Daten gefunden! Mindestens eine Datei wird benötigt.")
    
    return offtakers


def load_routes(scenario: str) -> gpd.GeoDataFrame:
    """Lädt berechnete Routen für ein Szenario"""
    route_file = ROUTES_DIR / f"routes_{scenario.lower()}.gpkg"
    
    if not route_file.exists():
        log(f"   ⚠️  Keine Routen gefunden für {scenario}: {route_file.name}")
        return gpd.GeoDataFrame(geometry=[], crs=TARGET_CRS)
    
    routes = gpd.read_file(route_file)
    routes = routes.to_crs(TARGET_CRS)
    log(f"   {scenario}: {len(routes)} Routen geladen")
    
    return routes


def find_offtakers_in_radius(wwtp_gdf: gpd.GeoDataFrame, 
                             offtakers: Dict[str, gpd.GeoDataFrame],
                             radius_m: float) -> pd.DataFrame:
    """
    Findet Abnehmer im Radius um jede Anlage
    
    Returns: DataFrame mit Spalten pro Abnehmer-Typ
    """
    log(f"\n3. Suche Abnehmer im {radius_m/1000:.1f} km Radius...")
    
    results = []
    
    for idx, wwtp in wwtp_gdf.iterrows():
        if wwtp.geometry is None:
            continue
        
        # Buffer um Anlage
        buffer = wwtp.geometry.buffer(radius_m)
        
        row_data = {KEY_COL: wwtp[KEY_COL]}
        
        # Für jeden Abnehmer-Typ
        for offtaker_type, offtaker_gdf in offtakers.items():
            # Finde Abnehmer im Buffer
            within = offtaker_gdf[offtaker_gdf.intersects(buffer)]
            
            # Anzahl
            row_data[f'{offtaker_type} in {radius_m/1000:.0f}km'] = len(within)
        
        results.append(row_data)
    
    result_df = pd.DataFrame(results)
    
    # Log Statistiken
    for offtaker_type in offtakers.keys():
        col = f'{offtaker_type} in {radius_m/1000:.0f}km'
        total = result_df[col].sum()
        with_offtakers = (result_df[col] > 0).sum()
        log(f"   {offtaker_type}: {with_offtakers} Anlagen mit Abnehmern (gesamt: {total:.0f})")
    
    return result_df


def find_offtakers_along_pipeline(wwtp_gdf: gpd.GeoDataFrame,
                                  routes: gpd.GeoDataFrame,
                                  offtakers: Dict[str, gpd.GeoDataFrame],
                                  buffer_m: float,
                                  radius_m: float) -> pd.DataFrame:
    """
    Findet Abnehmer entlang der EHB Pipeline mit Buffer
    Nur für Anlagen mit Built Flag = 1 und existierender Route
    Vermeidet Doppelzählung mit Radius-Suche
    
    Returns: DataFrame mit Spalten pro Abnehmer-Typ
    """
    log(f"\n4. Suche Abnehmer entlang EHB Pipeline (Buffer: {buffer_m/1000:.1f} km)...")
    
    if routes.empty:
        log(f"   Keine EHB Routen gefunden - überspringe Pipeline-Suche")
        return pd.DataFrame()
    
    # Prüfe Built Flag Spalte
    if BUILT_FLAG_COL not in wwtp_gdf.columns:
        log(f"   ⚠️  Built Flag Spalte '{BUILT_FLAG_COL}' nicht gefunden - überspringe")
        return pd.DataFrame()
    
    results = []
    
    # Finde passende ID-Spalte in routes
    route_id_col = None
    possible_id_cols = ['anlage_id', 'uwwtd code', 'uwwtd_code', 'wwtp_id', 'plant_id', 'id', 'site_id', 'code']
    
    # Zeige verfügbare Spalten für Debugging
    log(f"   Verfügbare Spalten in Routen: {list(routes.columns)}")
    
    for col in routes.columns:
        col_lower = str(col).lower().strip().replace('_', ' ').replace(' ', '')
        for possible in possible_id_cols:
            possible_normalized = possible.lower().replace('_', ' ').replace(' ', '')
            if col_lower == possible_normalized:
                route_id_col = col
                log(f"   ID-Spalte gefunden: '{col}'")
                break
        if route_id_col:
            break
    
    if not route_id_col:
        log(f"   ⚠️  Keine ID-Spalte in Routen gefunden")
        log(f"   Gesuchte Spalten: {possible_id_cols}")
        return pd.DataFrame()
    
    # Filtere nur Anlagen mit Built Flag = 1
    built_mask = wwtp_gdf[BUILT_FLAG_COL].fillna(0).astype(float) == 1
    wwtp_with_flag = wwtp_gdf[built_mask]
    log(f"   Anlagen mit Built Flag = 1: {len(wwtp_with_flag)}")
    
    processed_count = 0
    
    for idx, wwtp in wwtp_with_flag.iterrows():
        wwtp_id = wwtp[KEY_COL]
        
        # Finde Route für diese Anlage
        route = routes[routes[route_id_col] == wwtp_id]
        
        if route.empty:
            # Keine Route für diese Anlage - keine Pipeline-Abnehmer
            row_data = {KEY_COL: wwtp_id}
            for offtaker_type in offtakers.keys():
                row_data[f'{offtaker_type} along Pipeline (EHB)'] = 0
            results.append(row_data)
            continue
        
        processed_count += 1
        
        # Buffer um Pipeline
        pipeline_geom = route.geometry.iloc[0]
        pipeline_buffer = pipeline_geom.buffer(buffer_m)
        
        # Buffer um Anlage (für Doppelzählungs-Check)
        wwtp_buffer = wwtp.geometry.buffer(radius_m)
        
        row_data = {KEY_COL: wwtp_id}
        
        # Für jeden Abnehmer-Typ
        for offtaker_type, offtaker_gdf in offtakers.items():
            # Finde Abnehmer im Pipeline-Buffer
            along_pipeline = offtaker_gdf[offtaker_gdf.intersects(pipeline_buffer)]
            
            # Entferne Abnehmer, die bereits im Anlagen-Radius sind (Doppelzählung vermeiden)
            # Diese wurden bereits in der Radius-Suche gezählt
            in_wwtp_radius = along_pipeline[along_pipeline.intersects(wwtp_buffer)]
            unique_along_pipeline = along_pipeline[~along_pipeline.index.isin(in_wwtp_radius.index)]
            
            # Anzahl (nur die, die NICHT im Anlagen-Radius sind)
            row_data[f'{offtaker_type} along Pipeline (EHB)'] = len(unique_along_pipeline)
        
        results.append(row_data)
    
    log(f"   Routen verarbeitet: {processed_count}")
    
    result_df = pd.DataFrame(results)
    
    # Log Statistiken
    for offtaker_type in offtakers.keys():
        col = f'{offtaker_type} along Pipeline (EHB)'
        if col in result_df.columns:
            total = result_df[col].sum()
            with_offtakers = (result_df[col] > 0).sum()
            log(f"   {offtaker_type}: {with_offtakers} Anlagen mit zusätzlichen Abnehmern entlang Pipeline (gesamt: {total:.0f})")
    
    return result_df


def update_excel(df: pd.DataFrame, new_cols_df: pd.DataFrame):
    """Aktualisiert Excel mit neuen Spalten im H2 Logistics Sheet"""
    log("\n5. Aktualisiere Excel...")
    
    # Merge neue Spalten
    df_updated = df.merge(new_cols_df, on=KEY_COL, how='left')
    
    # Lade alle Sheets
    all_sheets = pd.read_excel(WWTP_XLSX, sheet_name=None)
    
    # Ersetze H2 Logistics Sheet
    all_sheets[PIPELINE_SHEET] = df_updated
    
    # Schreibe zurück
    with pd.ExcelWriter(WWTP_XLSX, engine='openpyxl', mode='w') as writer:
        for sheet_name, sheet_df in all_sheets.items():
            sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    log(f"   ✓ Excel aktualisiert (Sheet: '{PIPELINE_SHEET}'): {len(new_cols_df.columns) - 1} neue Spalten")
    
    # Zeige hinzugefügte Spalten
    new_cols = [col for col in new_cols_df.columns if col != KEY_COL]
    log(f"\n   Hinzugefügte Spalten:")
    for col in new_cols:
        log(f"      - {col}")


def main():
    log("=" * 80)
    log("WASSERSTOFF-ABNEHMER ANALYSE")
    log("=" * 80)
    
    # 1. Lade Daten
    df, wwtp_gdf = load_wwtp_data()
    offtakers = load_offtakers()
    
    # 2. Suche im Radius (für ALLE Anlagen)
    radius_m = RADIUS_KM * 1000
    radius_results = find_offtakers_in_radius(wwtp_gdf, offtakers, radius_m)
    
    # 3. Suche entlang EHB Pipeline (nur für Anlagen mit Built Flag = 1 und Route)
    routes = load_routes('EHB')
    buffer_m = PIPELINE_BUFFER_KM * 1000
    
    pipeline_results = find_offtakers_along_pipeline(
        wwtp_gdf, routes, offtakers, buffer_m, radius_m
    )
    
    # 4. Kombiniere Ergebnisse
    log("\n5. Kombiniere Ergebnisse...")
    if not pipeline_results.empty:
        final_results = radius_results.merge(pipeline_results, on=KEY_COL, how='left')
        # Fülle NaN mit 0 für Anlagen ohne Pipeline-Daten
        pipeline_cols = [col for col in pipeline_results.columns if col != KEY_COL]
        for col in pipeline_cols:
            final_results[col] = final_results[col].fillna(0).astype(int)
    else:
        final_results = radius_results
    
    log(f"   Finale Spalten: {len(final_results.columns) - 1}")
    
    # 5. Update Excel
    update_excel(df, final_results)
    
    log("\n" + "=" * 80)
    log("FERTIG!")
    log("=" * 80)


if __name__ == "__main__":
    main()
