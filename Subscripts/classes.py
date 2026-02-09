import pandas as pd
from pathlib import Path


def load_ranking_files():
    """Lädt die beiden Ranking-Dateien und fügt Ränge hinzu"""
    ranked_folder = Path("Output") / "MCA" / "Ranked"
    base_file = ranked_folder / "UWWTD_TP_Database_ranked_EHB_Basisszenario.xlsx"
    syn_file = ranked_folder / "UWWTD_TP_Database_ranked_EHB_Synergieszenario.xlsx"
    
    # Dateien prüfen
    if not base_file.exists():
        raise FileNotFoundError(f"Basis-Datei nicht gefunden: {base_file}")
    if not syn_file.exists():
        raise FileNotFoundError(f"Synergie-Datei nicht gefunden: {syn_file}")
    
    # Dateien laden
    base = pd.read_excel(base_file)
    syn = pd.read_excel(syn_file)
    
    # Ränge vergeben
    base = base.reset_index(drop=True)
    syn = syn.reset_index(drop=True)
    base["rank_base"] = base.index + 1
    syn["rank_syn"] = syn.index + 1
    
    return base, syn


def merge_scenarios(base, syn):
    """Führt beide Szenarien zusammen"""
    key_col = "UWWTD Code"
    
    # Merge über UWWTD Code
    merged = base.merge(
        syn[[key_col, "rank_syn"]],
        on=key_col,
        how="inner"
    )
    
    return merged


def select_top_candidates(merged):
    """Wählt Top 100 Kandidaten aus"""
    candidates = merged[
        (merged["rank_base"] <= 100) | (merged["rank_syn"] <= 100)
    ].copy()
    
    return candidates


def calculate_candidate_ranks(candidates):
    """Berechnet relative Ränge innerhalb der Kandidatenmenge."""
    # Ränge innerhalb Kandidaten
    candidates = candidates.sort_values("rank_base").reset_index(drop=True)
    candidates["rank_base_cand"] = candidates.index + 1
    
    candidates = candidates.sort_values("rank_syn").reset_index(drop=True)
    candidates["rank_syn_cand"] = candidates.index + 1
    
    # Prozentränge berechnen
    n_candidates = len(candidates)
    candidates["p_base"] = candidates["rank_base_cand"] / n_candidates
    candidates["p_syn"] = candidates["rank_syn_cand"] / n_candidates
    
    return candidates


def assign_performance_levels(candidates):
    """Weist Leistungsstufen gut/moderat/schwach zu."""
    def score_to_level(p):
        if p <= 0.25:
            return "gut"
        elif p <= 0.75:
            return "moderat"
        else:
            return "schwach"
    
    candidates["H2_level"] = candidates["p_base"].apply(score_to_level)
    candidates["Syn_level"] = candidates["p_syn"].apply(score_to_level)
    
    return candidates


def assign_classes(candidates):
    """Weist Klassen A-G basierend auf Leistungsstufen zu."""
    def assign_class(h2, syn):
        if h2 == "gut" and syn == "gut":
            return "A"  # beides gut
        if h2 == "gut" and syn == "moderat":
            return "B"  # H2 gut, Synergie moderat
        if h2 == "moderat" and syn == "gut":
            return "C"  # H2 moderat, Synergie gut
        if h2 == "gut" and syn == "schwach":
            return "D"  # nur H2 gut, Synergie schwach
        if h2 == "moderat" and syn == "moderat":
            return "E"  # in beiden moderat
        if h2 == "moderat" and syn == "schwach":
            return "F"  # H2 moderat, Synergie schwach
        if h2 == "schwach":
            return "G"  # H2 schwach (egal welche Synergie)
        # Fallback
        return "F"
    
    candidates["Klasse"] = candidates.apply(
        lambda row: assign_class(row["H2_level"], row["Syn_level"]), axis=1
    )
    
    # Delta-Rang berechnen
    candidates["delta_rank"] = candidates["rank_base"] - candidates["rank_syn"]
    
    return candidates


def save_results(candidates):
    """Speichert die Ergebnisse in Excel-Datei."""
    # Output-Ordner erstellen
    output_folder = Path("output")
    output_folder.mkdir(exist_ok=True)
    
    # Spalten für Ausgabe definieren
    output_cols = [
        "UWWTD Code", "Name", "Latitude", "Longitude", "Capacity/PE",
        "rank_base", "rank_syn", "rank_base_cand", "rank_syn_cand",
        "p_base", "p_syn", "H2_level", "Syn_level", "Klasse", "delta_rank"
    ]
    
    # Nur vorhandene Spalten verwenden
    available_cols = [col for col in output_cols if col in candidates.columns]
    
    # Nach Klasse sortieren und speichern
    result = candidates[available_cols].sort_values("Klasse").reset_index(drop=True)
    
    output_file = output_folder / "UWWTD_TP_Database_ranked_EHB_Top100_mit_Klassen_A_bis_G.xlsx"
    result.to_excel(output_file, index=False)
    
    return output_file, result


def print_summary(candidates, output_file):
    """Gibt eine Zusammenfassung der Ergebnisse aus."""
    total_candidates = len(candidates)
    class_counts = candidates["Klasse"].value_counts().sort_index()
    
    print(f"Kandidaten insgesamt: {total_candidates}")
    print("Klassenverteilung:")
    for klasse in ["A", "B", "C", "D", "E", "F", "G"]:
        count = class_counts.get(klasse, 0)
        percentage = (count / total_candidates) * 100 if total_candidates > 0 else 0
        print(f"  Klasse {klasse}: {count:2d} ({percentage:4.1f}%)")
    
    print(f"Datei gespeichert: {output_file}")


def main():
    """Hauptfunktion zur Ausführung des gesamten Workflows."""
    try:
        # 1. Dateien laden
        base, syn = load_ranking_files()
        
        # 2. Szenarien zusammenführen
        merged = merge_scenarios(base, syn)
        
        # 3. Top 100 Kandidaten auswählen
        candidates = select_top_candidates(merged)
        
        # 4. Relative Ränge innerhalb Kandidaten berechnen
        candidates = calculate_candidate_ranks(candidates)
        
        # Leistungsstufen zuweisen
        candidates = assign_performance_levels(candidates)
        
        # 6. Klassen A-G zuweisen
        candidates = assign_classes(candidates)
        
        # 7. Ergebnisse speichern
        output_file, result = save_results(candidates)
        
        # 8. Zusammenfassung ausgeben
        print_summary(candidates, output_file)
        
    except Exception as e:
        print(f"Fehler: {e}")


if __name__ == "__main__":
    main()