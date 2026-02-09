import numpy as np
import matplotlib.pyplot as plt
from electrolyse_decision import F

# Raster
distances = np.linspace(0, 120, 240)
pes = np.logspace(np.log10(3e5), np.log10(3e6), 240)

# Werte berechnen
Z = np.empty((len(distances), len(pes)))
for i, d in enumerate(distances):
    for j, pe in enumerate(pes):
        v = F(d, pe)
        Z[i, j] = np.nan if (v is None or not np.isfinite(v)) else float(v)

# Plot
plt.figure(figsize=(9, 6))
hm = plt.pcolormesh(pes, distances, np.ma.masked_invalid(Z),
                    shading="auto", cmap="RdYlGn", vmin=-0.3, vmax=0.3)
plt.colorbar(hm, label="Aeration cost savings - Hydrogen transport costs [€/kg H₂]")

# Break-even-Linie
Zc = np.nan_to_num(Z, nan=-1e6)
cs = plt.contour(pes, distances, Zc, levels=[0.0], colors="black", linewidths=2)
plt.clabel(cs, fmt="Break-even", inline=True, fontsize=9)

# Achsen & Ticks
plt.xscale("log")
plt.xlim(3e5, 3e6)
plt.xlabel("Population Equivalents PE")
plt.ylabel("Distance [km]")
plt.title("Rentability Electrolysis")

# Nur deine gewünschten Ticks
xticks = [3e5, 5e5, 1e6, 1.5e6, 2e6, 3e6]
xlabels = [f"{x/1e6:.1f}M" for x in xticks]
plt.xticks(xticks, xlabels)

# Minor-Ticks auf der x-Achse komplett deaktivieren
plt.gca().xaxis.set_minor_formatter(plt.NullFormatter())
plt.gca().xaxis.set_minor_locator(plt.NullLocator())

plt.tight_layout()
plt.show()