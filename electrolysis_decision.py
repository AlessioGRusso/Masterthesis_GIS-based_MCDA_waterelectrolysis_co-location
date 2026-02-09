import numpy as np

# Eingabedaten

# Distanz-Gitter (km)
d_grid = np.array([5.0, 25.0, 50.0], dtype=float)

# PE-Gitter (Anlagengrößen)
pe_grid = np.array([3e5, 5e5, 1e6, 1.5e6, 2e6], dtype=float)

# Matrix S[d, pe] mit Werten aus Graphen (€/kg H2)
# Zeilen = Distanz, Spalten = PE
S = np.array([
    [0.04, 0.075, 0.16, 0.2, 0.25],      # 5 km
    [-0.05, -0.02, 0.08, 0.120, 0.170],  # 25 km  
    [-0.130, -0.12, -0.04, 0.030, 0.080], # 50 km
], dtype=float)

# Max PE für Extrapolation
PE_MAX_EXTRAP = 4_000_000


# Hilfsfunktionen

def _row_eval(row_idx, pe):
    """Berechnet Wert für bestimmte Distanz bei beliebigem PE"""
    X = np.log(pe_grid)
    Y = S[row_idx, :]
    x = np.log(min(pe, PE_MAX_EXTRAP))
    i = int(np.clip(np.searchsorted(X, x) - 1, 0, len(X)-2))
    u = (x - X[i]) / (X[i+1] - X[i])
    return (1-u)*Y[i] + u*Y[i+1]


def _interp_S(d, pe):
    """Interpoliert in Distanz und PE"""
    # Werte für jede Distanz
    s5  = _row_eval(0, pe)  # 5 km
    s25 = _row_eval(1, pe)
    s50 = _row_eval(2, pe)

    if d <= 5.0:
        m = (s25 - s5) / (25.0 - 5.0)
        m = min(m, 0.0)   # nicht steigender Verlauf
        return s5 + m * (d - 5.0)

    if d >= 50.0:
        m = (s50 - s25) / (50.0 - 25.0)
        m = min(m, 0.0)
        return s50 + m * (d - 50.0)

    # 5..25 km
    if d <= 25.0:
        t = (d - 5.0) / (25.0 - 5.0)
        return (1 - t) * s5 + t * s25
    # 25..50 km
    else:
        t = (d - 25.0) / (50.0 - 25.0)
        return (1 - t) * s25 + t * s50


# ======================================================
# Hauptfunktionen
# ======================================================

def F(d, pe, safety_margin=0.0):
    """
    Entscheidungswert F(d, PE) in €/kg H2.
    >= 0 -> rentabel.
    """
    if pe < 3e5:
        return np.nan  # unter 300k nicht betrachtet
    return _interp_S(float(d), float(pe)) - safety_margin


def decision(d, pe, threshold=0.0):
    """
    Entscheidung für gegebene Distanz & PE.
    threshold >0 macht Entscheidung konservativer.
    """
    v = F(d, pe)
    if np.isfinite(v) and v >= threshold:
        return "bauen", v
    else:
        return "nicht bauen", v


# ======================================================
# Beispiele
# ======================================================
if __name__ == "__main__":
    tests = [
        (10, 500_000),
        (25, 500_000),
        (25, 2_000_000),
        (60, 2_000_000),
        (60, 3_000_000),   # extrapoliert
        (80, 4_000_000),
    ]
    for d, pe in tests:
        print(d, "km,", pe, "PE ->", decision(d, pe))