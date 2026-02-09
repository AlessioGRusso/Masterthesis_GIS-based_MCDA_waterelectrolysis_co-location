from __future__ import annotations

import json
import subprocess
import sys
import tkinter as tk
from dataclasses import dataclass, field, asdict
from tkinter import ttk, filedialog, messagebox
from typing import Dict, List, Optional
from pathlib import Path

# Optionaler Import der Monte-Carlo-Erweiterung
try:
    from monte_carlo_extension import MonteCarloWindow

    MONTE_CARLO_AVAILABLE = True
except ImportError:
    MONTE_CARLO_AVAILABLE = False
    print("Monte Carlo Extension nicht verfügbar")

# Feste KPI-Definitionen (nicht über die GUI editierbar)
FIXED_KPIS = {
    "H2 Renewables Potential": [
        {"name": "Calculated LCOH", "column": "LCOH [€/kg]", "sheet": "H2 Renewables", "direction": "cost"}
    ],
    "H2 Logistics": [
        {"name": "Pipeline Cost", "column": "Price (EHB)", "sheet": "H2 Logistics", "direction": "cost"},
        {"name": "Offtakers Potential", "column": "Offtakers Potential", "sheet": "H2 Logistics",
         "direction": "benefit"}
    ],
    "Grid Electricity": [
        {"name": "Distance to HV power line", "column": "Direct Distance to Power Network [km]",
         "sheet": "Grid Energy Connection", "direction": "cost"},
        {"name": "Distance to HV Substation", "column": "Distance to Nearest Substation [km]",
         "sheet": "Grid Energy Connection", "direction": "cost"}
    ],
    "District Heating": [
        {"name": "Max. Flow Temperature", "column": "Max. Flow temperature", "sheet": "District Heating",
         "direction": "benefit"},
        {"name": "Distance to District Heating Network", "column": "Distance to network [km]",
         "sheet": "District Heating", "direction": "cost"}
    ],
    "Oxygen": [
        {"name": "Oxygen Usage Potential", "column": "Oxygen_Usage_Potential", "sheet": "General Data",
         "direction": "benefit"}
    ],

    "Risk": [
        {"name": "Flood Risk (inverted)", "column": "Distance to possible flood area [m]", "sheet": "Risks",
         "direction": "benefit", "w_min": 0.3, "w_max": 0.35},
        {"name": "Residential Risk (inverted)", "column": "Distance to residential area [m]", "sheet": "Risks",
         "direction": "benefit", "w_min": 0.3, "w_max": 0.35},
        {"name": "Protected Area Risk (inverted)", "column": "Distance to protected area [m]", "sheet": "Risks",
         "direction": "benefit", "w_min": 0.3, "w_max": 0.35}
    ]
}

# Feste Risiko-KPIs inkl. Default-Thresholds
FIXED_RISK_KPIS = [
    {"name": "Flood (distance)", "column": "Distance to possible flood area [m]", "safe": 500, "critical": 100},
    {"name": "Residential (distance)", "column": "Distance to residential area [m]", "safe": 500, "critical": 200},
    {"name": "Protected Area", "column": "Distance to protected area [m]", "safe": 500, "critical": 200}
]


# ---------------------- Datenmodelle ----------------------
@dataclass
class KPI:
    name: str
    column: str
    sheet: str
    direction: str = "benefit"
    weight: Optional[float] = None
    w_min: Optional[float] = None
    w_max: Optional[float] = None


@dataclass
class Category:
    name: str
    sheet: str = ""
    method: str = "custom"
    kpis: List[KPI] = field(default_factory=list)

    # Gewichtsbandbreiten auf Kategorieebene
    weight: Optional[float] = None
    w_min: Optional[float] = None
    w_max: Optional[float] = None

    # Subkategorien (hier aktuell nicht aktiv genutzt)
    subcategories: Dict[str, "Category"] = field(default_factory=dict)
    sub_agg_method: str = "weighted_average"  # "weighted_average" | "average" | "sum"
    sub_agg_weights: Dict[str, float] = field(default_factory=dict)

    # Bedingte Logik zur Aktivierung/Bewertung (z. B. District Heating)
    conditional_check: Optional[Dict[str, any]] = None  # {"type": "...", "columns": ["..."]}


@dataclass
class ProjectConfig:
    excel_path: str = "Output/UWWTD_TP_Database.xlsx"
    id_columns: List[str] = field(default_factory=lambda: ["Site_ID", "Name"])
    default_sheet: Optional[str] = None

    # Normalisierung/Imputation
    winsorize_pct: Optional[float] = 0.0
    imputation_enabled: bool = True
    imputation_method: str = "median"

    categories: Dict[str, Category] = field(default_factory=lambda: {
        "H2 Renewables Potential": Category(name="H2 Renewables Potential", sheet="H2 Renewables", method="custom"),
        "H2 Logistics": Category(name="H2 Logistics", sheet="H2 Logistics", method="custom"),
        "Grid Electricity": Category(name="Grid Electricity", sheet="Grid Energy Connection", method="custom"),
        "District Heating": Category(
            name="District Heating",
            sheet="District Heating",
            method="custom",
            conditional_check={
                "type": "district_heating_automatic",
                "description": "Automatic logic based on Network and Connection columns"
            }
        ),
        "Oxygen": Category(name="Oxygen", sheet="General Data", method="custom"),
        "Risk": Category(name="Risk", sheet="Risks", method="custom"),
    })

    # Anzeige-/Verarbeitungsreihenfolge
    category_order: List[str] = field(
        default_factory=lambda: ["H2 Renewables Potential", "H2 Logistics", "Grid Electricity", "District Heating",
                                 "Oxygen", "Risk"])

    # Risiko-Konfiguration (Sheet und Default-Schwellenwerte)
    risk_sheet: str = "Risks"
    risk_cols: Dict[str, str] = field(default_factory=lambda: {
        "flood": "",
        "residential": "",
        "protected": "",
        "area": ""
    })
    risk_thresholds: Dict[str, Dict[str, float]] = field(default_factory=lambda: {
        "flood": {"safe": 500, "critical": 100},
        "residential": {"safe": 500, "critical": 200},
        "protected": {"safe": 500, "critical": 200},
        "area": {"sufficient": 10000, "critical": 0}
    })
    risk_weights: Dict[str, Dict[str, Optional[float]]] = field(default_factory=lambda: {
        "flood": {"weight": 0.333, "w_min": None, "w_max": None},
        "residential": {"weight": 0.333, "w_min": None, "w_max": None},
        "protected": {"weight": 0.334, "w_min": None, "w_max": None}
    })


# ---------------------- GUI ----------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("MCA – Config & MC Hub (Custom Weights Only)")
        self.geometry("1100x850")
        self.minsize(980, 680)

        # Styles für visuelle Gruppierung
        style = ttk.Style()
        style.configure("Category.TLabelframe", background="#f0f0f0")
        style.configure("Category.TLabelframe.Label", background="#f0f0f0", font=("", 11, "bold"))

        self.state = ProjectConfig()

        # GUI-Variablen: Kategoriegewichte (Min/Max)
        self.category_weight_vars: Dict[str, Dict[str, tk.StringVar]] = {}

        # GUI-Aufbau
        self._build_scrollable()
        self._build_project(self.main)
        self._build_categories(self.main)
        self._build_norm_impute(self.main)
        self._build_export(self.main)
        self._build_fixed_footer()

        self.after(0, self._center_on_screen)
        self.after(500, self._auto_load_config)

    # ----- Scrollbarer Content-Bereich -----
    def _build_scrollable(self):
        self.content_wrapper = ttk.Frame(self)
        self.content_wrapper.pack(side="top", fill=tk.BOTH, expand=True)

        self.canvas = tk.Canvas(self.content_wrapper, borderwidth=0, highlightthickness=0)
        vsb = ttk.Scrollbar(self.content_wrapper, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right", fill="y")
        self.canvas.pack(side="left", fill="both", expand=True)

        self.main = ttk.Frame(self.canvas)
        self.win_id = self.canvas.create_window((0, 0), window=self.main, anchor="nw")

        # Scrollregion und Fensterbreite synchronisieren
        def on_configure(_=None):
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))
            self.canvas.itemconfigure(self.win_id, width=self.canvas.winfo_width())

        self.main.bind("<Configure>", on_configure)
        self.canvas.bind("<Configure>", on_configure)

        # Maus-Scrollrad aktivieren
        def on_mousewheel(event):
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        self.canvas.bind_all("<MouseWheel>", on_mousewheel)

    def _center_on_screen(self):
        # Fenster zentrieren
        self.update_idletasks()
        w, h = self.winfo_width(), self.winfo_height()
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        self.geometry(f"{w}x{h}+{(sw - w) // 2}+{(sh - h) // 2}")

    def _build_project(self, parent):
        box = ttk.LabelFrame(parent, text="Project settings", relief="solid", borderwidth=1,
                             style="Category.TLabelframe")
        box.pack(fill=tk.X, padx=8, pady=(8, 12))
        for i in range(3): box.columnconfigure(i, weight=1)

        ttk.Label(box, text="Excel file").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        self.var_excel = tk.StringVar(value=self.state.excel_path)
        ttk.Entry(box, textvariable=self.var_excel).grid(row=0, column=1, sticky="we", padx=8)
        ttk.Button(box, text="Browse", command=self._choose_excel).grid(row=0, column=2, padx=8)

        ttk.Label(box, text="ID columns (comma-separated)").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        self.var_ids = tk.StringVar(value=", ".join(self.state.id_columns))
        ttk.Entry(box, textvariable=self.var_ids).grid(row=1, column=1, sticky="we", padx=8)

    def _choose_excel(self):
        # Excel-Datei auswählen
        path = filedialog.askopenfilename(filetypes=[("Excel", "*.xlsx *.xlsm *.xls")])
        if path: self.var_excel.set(path)

    def _build_categories(self, parent):
        # KPI-Variablen je Kategorie
        self.kpi_vars = {}

        # Builder für Kategorie-UI (Min/Max Gewichte)
        def build_category_ui(container: ttk.Frame, key: str, default_sheet: str, is_subcategory: bool = False):
            """Erstellt UI für eine (Sub-)Kategorie ohne Base-Weight."""
            display_name = key if '::' not in key else key.split('::')[1]

            # KPI-Liste aus fixen Definitionen
            fixed_kpis = FIXED_KPIS.get(display_name, [])

            lf = ttk.LabelFrame(container, text=f"{display_name}", relief="solid", borderwidth=1)
            lf.pack(fill=tk.X, padx=8, pady=(4, 12))

            # Style anwenden (falls verfügbar)
            try:
                lf.configure(style="Category.TLabelframe")
            except Exception as e:
                print(f"Style config error: {e}")

            sheet_var = tk.StringVar(value=default_sheet)

            # Kategoriegewichte abhängig von Kategorie/Subkategorie anzeigen
            show_weights = not is_subcategory or display_name in ["Grid Electricity", "EE Potential"]

            if show_weights:
                weight_frame = ttk.Frame(lf)
                weight_frame.grid(row=0, column=0, columnspan=6, sticky="we", padx=8, pady=(8, 4))

                ttk.Label(weight_frame, text="Category Weight:", font=("", 9, "bold")).pack(side=tk.LEFT, padx=(0, 10))
                ttk.Label(weight_frame, text="Min. Weight:").pack(side=tk.LEFT, padx=(0, 4))
                var_cat_wmin = tk.StringVar()
                ttk.Entry(weight_frame, textvariable=var_cat_wmin, width=12).pack(side=tk.LEFT, padx=(0, 10))
                ttk.Label(weight_frame, text="Max. Weight:").pack(side=tk.LEFT, padx=(0, 4))
                var_cat_wmax = tk.StringVar()
                ttk.Entry(weight_frame, textvariable=var_cat_wmax, width=12).pack(side=tk.LEFT)

                self.category_weight_vars[key] = {
                    'w_min': var_cat_wmin,
                    'w_max': var_cat_wmax
                }

            start_row = 1 if show_weights else 0
            ttk.Separator(lf, orient="horizontal").grid(row=start_row, column=0, columnspan=6, sticky="we", padx=8,
                                                        pady=(4, 8))

            # Sonderlayout: Risk (Thresholds + Gewichte)
            if display_name == "Risk":

                # Variablencontainer für Risk-Thresholds
                if not hasattr(self, 'risk_vars'):
                    self.risk_vars = {}

                info_row = start_row + 1
                info_label = ttk.Label(
                    lf,
                    text="Risk Thresholds (linear interpolation): > Safe = Score 1.0, < Critical = Score 0.0",
                    font=("", 8, "italic"),
                    foreground="gray"
                )
                info_label.grid(row=info_row, column=0, columnspan=7, sticky="w", padx=8, pady=(4, 8))

                header_row = info_row + 1
                ttk.Label(lf, text="KPI", font=("", 9, "bold")).grid(row=header_row, column=0, sticky="w", padx=(8, 4),
                                                                     pady=4)
                ttk.Label(lf, text="Safe/Sufficient", font=("", 9, "bold")).grid(row=header_row, column=1, sticky="w",
                                                                                 padx=4, pady=4)
                ttk.Label(lf, text="Critical", font=("", 9, "bold")).grid(row=header_row, column=2, sticky="w", padx=4,
                                                                          pady=4)
                ttk.Label(lf, text="Direction", font=("", 9, "bold")).grid(row=header_row, column=3, sticky="w", padx=4,
                                                                           pady=4)
                ttk.Label(lf, text="Min. Weight", font=("", 9, "bold")).grid(row=header_row, column=4, sticky="w",
                                                                             padx=4, pady=4)
                ttk.Label(lf, text="Max. Weight", font=("", 9, "bold")).grid(row=header_row, column=5, sticky="w",
                                                                             padx=(4, 8), pady=4)

                if key not in self.kpi_vars:
                    self.kpi_vars[key] = {"sheet": sheet_var, "kpis": []}

                for i, risk_kpi in enumerate(FIXED_RISK_KPIS, start=header_row + 1):
                    risk_key = risk_kpi["name"].split()[0].lower()

                    # KPI-Label
                    ttk.Label(lf, text=risk_kpi["name"]).grid(row=i, column=0, sticky="w", padx=(8, 4), pady=2)

                    # Safe/Sufficient
                    safe_var = tk.StringVar(value=str(int(risk_kpi["safe"])))
                    ttk.Entry(lf, textvariable=safe_var, width=12).grid(row=i, column=1, sticky="w", padx=4, pady=2)

                    # Critical
                    crit_var = tk.StringVar(value=str(int(risk_kpi["critical"])))
                    ttk.Entry(lf, textvariable=crit_var, width=12).grid(row=i, column=2, sticky="w", padx=4, pady=2)

                    self.risk_vars[risk_key] = {
                        "column": risk_kpi["column"],
                        "safe": safe_var,
                        "critical": crit_var
                    }

                    # Mapping Anzeige-Name -> FIXED_KPIS Name
                    kpi_def = None
                    risk_name_map = {
                        "Flood (distance)": "Flood Risk (inverted)",
                        "Residential (distance)": "Residential Risk (inverted)",
                        "Protected Area": "Protected Area Risk (inverted)"
                    }
                    mapped_name = risk_name_map.get(risk_kpi["name"])
                    for fkpi in fixed_kpis:
                        if fkpi["name"] == mapped_name:
                            kpi_def = fkpi
                            break

                    if kpi_def:
                        # Richtung (readonly)
                        dir_var = tk.StringVar(value=kpi_def["direction"])
                        ttk.Entry(lf, textvariable=dir_var, width=10, state="readonly").grid(row=i, column=3,
                                                                                             sticky="w", padx=4, pady=2)

                        # Gewichte (Min/Max)
                        wmin_var = tk.StringVar()
                        ttk.Entry(lf, textvariable=wmin_var, width=12).grid(row=i, column=4, sticky="w", padx=4, pady=2)
                        wmax_var = tk.StringVar()
                        ttk.Entry(lf, textvariable=wmax_var, width=12).grid(row=i, column=5, sticky="w", padx=(4, 8),
                                                                            pady=2)

                        self.kpi_vars[key]["kpis"].append({
                            "name": kpi_def["name"],
                            "column": kpi_def["column"],
                            "direction": dir_var,
                            "w_min": wmin_var,
                            "w_max": wmax_var
                        })

                # Spaltenlayout
                lf.columnconfigure(0, weight=0, minsize=200)
                lf.columnconfigure(1, weight=0, minsize=110)
                lf.columnconfigure(2, weight=0, minsize=80)
                lf.columnconfigure(3, weight=0, minsize=80)
                lf.columnconfigure(4, weight=0, minsize=100)
                lf.columnconfigure(5, weight=0, minsize=100)

                return None, lf

            # Sonderfall: Kategorie mit genau einem KPI
            if len(fixed_kpis) == 1:
                start_row += 1
                info_label = ttk.Label(
                    lf,
                    text=f"KPI: {fixed_kpis[0]['name']} (Direction: {fixed_kpis[0]['direction']})",
                    font=("", 9, "italic"),
                    foreground="gray"
                )
                info_label.grid(row=start_row, column=0, columnspan=6, sticky="w", padx=8, pady=(4, 8))

                if key not in self.kpi_vars:
                    self.kpi_vars[key] = {"sheet": sheet_var, "kpis": []}

                kpi_def = fixed_kpis[0]
                dir_var = tk.StringVar(value=kpi_def["direction"])
                wmin_var = tk.StringVar()
                wmax_var = tk.StringVar()

                self.kpi_vars[key]["kpis"].append({
                    "name": kpi_def["name"],
                    "column": kpi_def["column"],
                    "direction": dir_var,
                    "w_min": wmin_var,
                    "w_max": wmax_var
                })
            else:
                # Standardlayout: mehrere KPIs
                start_row += 1
                ttk.Label(lf, text="KPI name", font=("", 9, "bold")).grid(row=start_row, column=0, sticky="w",
                                                                          padx=(8, 4), pady=4)
                ttk.Label(lf, text="Direction", font=("", 9, "bold")).grid(row=start_row, column=1, sticky="w", padx=4,
                                                                           pady=4)

                weight_header_frame = ttk.Frame(lf)
                weight_header_frame.grid(row=start_row, column=2, columnspan=2, sticky="w", padx=4, pady=4)
                ttk.Label(weight_header_frame, text="Min. Weight", font=("", 9, "bold")).pack(side=tk.LEFT,
                                                                                              padx=(0, 10))
                ttk.Label(weight_header_frame, text="Max. Weight", font=("", 9, "bold")).pack(side=tk.LEFT)

                if key not in self.kpi_vars:
                    self.kpi_vars[key] = {"sheet": sheet_var, "kpis": []}

                for i, kpi_def in enumerate(fixed_kpis, start=start_row + 1):
                    ttk.Label(lf, text=kpi_def["name"]).grid(row=i, column=0, sticky="w", padx=(8, 4), pady=2)

                    dir_var = tk.StringVar(value=kpi_def["direction"])
                    ttk.Entry(lf, textvariable=dir_var, width=10, state="readonly").grid(row=i, column=1, sticky="w",
                                                                                         padx=4, pady=2)

                    weight_frame = ttk.Frame(lf)
                    weight_frame.grid(row=i, column=2, columnspan=2, sticky="w", padx=4, pady=2)

                    wmin_var = tk.StringVar()
                    ttk.Entry(weight_frame, textvariable=wmin_var, width=12).pack(side=tk.LEFT, padx=(0, 10))
                    wmax_var = tk.StringVar()
                    ttk.Entry(weight_frame, textvariable=wmax_var, width=12).pack(side=tk.LEFT)

                    self.kpi_vars[key]["kpis"].append({
                        "name": kpi_def["name"],
                        "column": kpi_def["column"],
                        "direction": dir_var,
                        "w_min": wmin_var,
                        "w_max": wmax_var
                    })

                lf.columnconfigure(0, weight=0, minsize=250)
                lf.columnconfigure(1, weight=0, minsize=80)
                lf.columnconfigure(2, weight=1, minsize=250)

            return None, lf

        # Kategorien in definierter Reihenfolge anzeigen
        display_order = ["H2 Renewables Potential", "H2 Logistics", "Grid Electricity", "District Heating", "Oxygen",
                         "Risk"]

        container = ttk.Frame(parent)
        container.pack(fill=tk.X, pady=(0, 8))

        for cat_name in display_order:
            if cat_name in self.state.categories:
                build_category_ui(
                    parent,
                    key=cat_name,
                    default_sheet=self.state.categories[cat_name].sheet,
                    is_subcategory=False
                )

    # ---------- Abschnitt: Normalisierung & Imputation ----------
    def _build_norm_impute(self, parent):
        box = ttk.LabelFrame(parent, text="Normalize & Impute", relief="solid", borderwidth=1,
                             style="Category.TLabelframe")
        box.pack(fill=tk.X, padx=8, pady=(4, 12))

        row1 = ttk.Frame(box)
        row1.pack(fill=tk.X, padx=8, pady=6)

        ttk.Label(row1, text="Winsorize pct (e.g., 0.05)").pack(side=tk.LEFT, padx=(0, 8))
        self.var_wins = tk.DoubleVar(value=float(self.state.winsorize_pct or 0.0))
        ttk.Entry(row1, textvariable=self.var_wins, width=10).pack(side=tk.LEFT, padx=(0, 30))

        self.var_imp = tk.BooleanVar(value=self.state.imputation_enabled)
        ttk.Checkbutton(row1, text="Imputation enabled", variable=self.var_imp).pack(side=tk.LEFT, padx=(0, 30))

        ttk.Label(row1, text="Imputation method").pack(side=tk.LEFT, padx=(0, 8))
        self.var_imp_method = tk.StringVar(value=self.state.imputation_method)
        ttk.Combobox(row1, values=["median", "mean"], textvariable=self.var_imp_method, state="readonly",
                     width=12).pack(side=tk.LEFT)

    # ---------- Abschnitt: Export ----------
    def _build_export(self, parent):
        box = ttk.LabelFrame(parent, text="Export & Analysis", relief="solid", borderwidth=1,
                             style="Category.TLabelframe")
        box.pack(fill=tk.X, padx=8, pady=(4, 12))
        btn_frame = ttk.Frame(box);
        btn_frame.grid(row=0, column=0, sticky="e", padx=8, pady=6)

        # Export der Konfiguration als JSON
        ttk.Button(btn_frame, text="Export JSON", command=self._export_json).pack(side=tk.LEFT, padx=(0, 8))

        # Monte-Carlo-Parameterfenster (nur bei verfügbarer Extension)
        if MONTE_CARLO_AVAILABLE:
            self.mc_button = ttk.Button(btn_frame, text="Monte Carlo Parameters",
                                        command=self._start_monte_carlo, state="disabled")
            self.mc_button.pack(side=tk.LEFT)

        box.columnconfigure(0, weight=1)

    # ---------- Fixer Footer ----------
    def _build_fixed_footer(self):
        bar = ttk.Frame(self, padding=(8, 6));
        bar.pack(side="bottom", fill="x")
        ttk.Separator(bar, orient="horizontal").pack(side="top", fill="x", pady=(0, 6))
        ttk.Button(bar, text="Back to Overview", command=self._back_to_overview).pack(side="right", padx=8)

    # ---------- Collect & Export ----------
    def _collect_config(self) -> ProjectConfig:
        # GUI-Eingaben in ProjectConfig übertragen
        cfg = ProjectConfig()
        cfg.excel_path = self.var_excel.get().strip()
        cfg.id_columns = [s.strip() for s in self.var_ids.get().split(",") if s.strip()]
        cfg.default_sheet = None  # Nicht genutzt

        cfg.winsorize_pct = float(self.var_wins.get()) if str(self.var_wins.get()) not in ("", "None") else None
        cfg.imputation_enabled = bool(self.var_imp.get())
        cfg.imputation_method = self.var_imp_method.get()

        cats: Dict[str, Category] = {}

        # Kategorien einsammeln (Energy wird übersprungen)
        for cname in self.state.category_order:
            if cname == "Energy":
                continue

            if cname not in self.kpi_vars:
                continue

            cat_data = self.kpi_vars[cname]
            c = Category(name=cname, sheet=cat_data["sheet"].get(), method="custom")

            # Kategoriegewichte (nur Min/Max)
            if cname in self.category_weight_vars:
                cat_vars = self.category_weight_vars[cname]
                c.w_min = float(cat_vars['w_min'].get().replace(',', '.')) if cat_vars['w_min'].get().strip() else None
                c.w_max = float(cat_vars['w_max'].get().replace(',', '.')) if cat_vars['w_max'].get().strip() else None
                c.weight = None  # Sampling im Monte-Carlo

            # Conditional Check für District Heating
            if cname == "District Heating":
                c.conditional_check = {
                    "type": "district_heating_automatic",
                    "description": "Automatic logic based on Network and Connection columns"
                }

            # KPIs (Spalten/Sheets sind fest, Gewichte variabel)
            for kpi_vars in cat_data["kpis"]:
                c.kpis.append(KPI(
                    name=kpi_vars["name"],
                    column=kpi_vars["column"],
                    sheet=cat_data["sheet"].get(),
                    direction=kpi_vars["direction"].get(),
                    weight=None,
                    w_min=float(kpi_vars["w_min"].get().replace(',', '.')) if kpi_vars["w_min"].get().strip() else None,
                    w_max=float(kpi_vars["w_max"].get().replace(',', '.')) if kpi_vars["w_max"].get().strip() else None,
                ))
            cats[cname] = c

        cfg.categories = cats

        # Risiken: feste Sheet-Namen, Thresholds aus GUI, Weights aus Risk-KPIs
        cfg.risk_sheet = "Risks"
        cfg.risk_cols = {}
        cfg.risk_thresholds = {}
        cfg.risk_weights = {}

        # Thresholds sammeln
        for risk_kpi in FIXED_RISK_KPIS:
            risk_key = risk_kpi["name"].split()[0].lower()
            vars_dict = self.risk_vars[risk_key]

            cfg.risk_cols[risk_key] = vars_dict["column"]

            if risk_key == "area":
                cfg.risk_thresholds[risk_key] = {
                    "sufficient": float(vars_dict["safe"].get().replace(',', '.')) if vars_dict[
                        "safe"].get().strip() else 10000,
                    "critical": float(vars_dict["critical"].get().replace(',', '.')) if vars_dict[
                        "critical"].get().strip() else 0
                }
            else:
                cfg.risk_thresholds[risk_key] = {
                    "safe": float(vars_dict["safe"].get().replace(',', '.')) if vars_dict[
                        "safe"].get().strip() else 500,
                    "critical": float(vars_dict["critical"].get().replace(',', '.')) if vars_dict[
                        "critical"].get().strip() else 200
                }

        # Risk-Weights aus Risk-KPI-Liste ableiten
        if "Risk" in cats and cats["Risk"].kpis:
            for kpi in cats["Risk"].kpis:
                risk_key_map = {
                    "Flood Risk (inverted)": "flood",
                    "Residential Risk (inverted)": "residential",
                    "Protected Area Risk (inverted)": "protected"
                }
                risk_key = risk_key_map.get(kpi.name)
                if risk_key:
                    cfg.risk_weights[risk_key] = {
                        "weight": kpi.weight if kpi.weight is not None else 0.25,
                        "w_min": kpi.w_min,
                        "w_max": kpi.w_max
                    }

        return cfg

    def _export_json(self):
        try:
            cfg = self._collect_config()
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            print(f"ERROR in _collect_config:\n{error_details}")
            messagebox.showerror("Collection Error",
                                 f"Error collecting config:\n{str(e)}\n\n{type(e).__name__}\n\nSee console for details.")
            return

        # Vereinfachter Export: Fokus auf Gewichte
        export_data = {
            "excel_path": cfg.excel_path,
            "id_columns": cfg.id_columns,
            "winsorize_pct": cfg.winsorize_pct,
            "imputation_enabled": cfg.imputation_enabled,
            "imputation_method": cfg.imputation_method,
            "category_weights": {},
            "kpi_weights": {},
            "risk_weights": cfg.risk_weights
        }

        # Kategorie- und KPI-Gewichte extrahieren
        for cat_name, category in cfg.categories.items():
            if category.weight is not None or category.w_min is not None or category.w_max is not None:
                export_data["category_weights"][cat_name] = {
                    "weight": category.weight,
                    "w_min": category.w_min,
                    "w_max": category.w_max
                }

            for kpi in category.kpis:
                kpi_key = f"{cat_name}::{kpi.name}"
                export_data["kpi_weights"][kpi_key] = {
                    "weight": kpi.weight,
                    "w_min": kpi.w_min,
                    "w_max": kpi.w_max
                }

        # Vollständige Config für Kompatibilität/Processing
        try:
            full_data = asdict(cfg)
        except Exception as e:
            print(f"Warning: Could not convert config to dict: {e}")
            full_data = export_data.copy()
            full_data["categories"] = {}
            for cat_name, category in cfg.categories.items():
                full_data["categories"][cat_name] = {
                    "name": category.name,
                    "sheet": category.sheet,
                    "method": category.method,
                    "weight": category.weight,
                    "w_min": category.w_min,
                    "w_max": category.w_max,
                    "kpis": [
                        {
                            "name": kpi.name,
                            "column": kpi.column,
                            "sheet": kpi.sheet,
                            "direction": kpi.direction,
                            "weight": kpi.weight,
                            "w_min": kpi.w_min,
                            "w_max": kpi.w_max
                        }
                        for kpi in category.kpis
                    ]
                }

        out_path = Path("Output") / "MCA" / "Weights_MC"
        try:
            out_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Error creating directory {out_path}: {e}")
            messagebox.showerror("Directory Error", f"Could not create output directory:\n{out_path}\n\n{e}")
            return

        # Datei: weights_config.json (Gewichte)
        weights_file = out_path / "weights_config.json"
        try:
            print(f"Attempting to write to: {weights_file}")
            with open(str(weights_file), "w", encoding="utf-8") as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            print(f"Successfully wrote weights_config.json")

            # Datei: kpi_config.json (vollständig)
            full_file = out_path / "kpi_config.json"
            print(f"Attempting to write to: {full_file}")
            with open(str(full_file), "w", encoding="utf-8") as f:
                json.dump(full_data, f, indent=2, ensure_ascii=False)
            print(f"Successfully wrote kpi_config.json")

            self._show_silent_info("Export", f"Weights saved to: {weights_file}\nFull config saved to: {full_file}")

            # Button-Status aktualisieren
            self._update_monte_carlo_button()

            # Optionaler Direktstart Monte Carlo bei ausreichender Konfiguration
            if self._is_config_complete(cfg) and MONTE_CARLO_AVAILABLE:
                if messagebox.askyesno("Monte Carlo Analysis",
                                       "Configuration is complete with all KPIs configured.\n\n"
                                       "Would you like to proceed directly to Monte Carlo analysis?"):
                    self._start_monte_carlo()

        except Exception as e:
            messagebox.showerror("Export failed", str(e))

    def _is_config_complete(self, cfg) -> bool:
        """Vollständigkeitsprüfung: ausreichend KPI-Gewichte für Monte Carlo."""
        total_kpis = 0
        configured_kpis = 0

        for cat_name, category in cfg.categories.items():
            # Single-KPI: Kategorie-Min/Max verwenden
            if len(category.kpis) == 1:
                total_kpis += 1
                if category.w_min is not None and category.w_max is not None:
                    configured_kpis += 1
                    print(
                        f"✓ {cat_name}::{category.kpis[0].name} - using category weights (w_min={category.w_min}, w_max={category.w_max})")
                else:
                    print(f"✗ {cat_name}::{category.kpis[0].name} - NO CATEGORY WEIGHTS")
            else:
                # Multi-KPI: KPI-Min/Max prüfen
                for kpi in category.kpis:
                    total_kpis += 1
                    if kpi.w_min is not None and kpi.w_max is not None:
                        configured_kpis += 1
                        print(f"✓ {cat_name}::{kpi.name} - w_min={kpi.w_min}, w_max={kpi.w_max}")
                    elif kpi.weight is not None and kpi.weight > 0:
                        configured_kpis += 1
                        print(f"✓ {cat_name}::{kpi.name} - weight={kpi.weight}")
                    else:
                        print(f"✗ {cat_name}::{kpi.name} - NO WEIGHTS")

        completion_pct = (configured_kpis / total_kpis * 100) if total_kpis > 0 else 0
        print(f"\nConfiguration: {configured_kpis}/{total_kpis} KPIs configured ({completion_pct:.1f}%)")

        # Kriterium: mindestens 80% der KPIs gewichtet
        return total_kpis > 0 and (configured_kpis / total_kpis) >= 0.8

    def _update_monte_carlo_button(self):
        """Aktualisiert den Button-Status basierend auf gespeicherter oder aktueller Konfiguration."""
        if MONTE_CARLO_AVAILABLE and hasattr(self, 'mc_button'):
            try:
                config_path = Path("Output/MCA/Weights_MC/kpi_config.json")
                if config_path.exists():
                    self.mc_button.config(state="normal")
                    print("✓ Monte Carlo button ENABLED (saved config found)")
                    return

                cfg = self._collect_config()
                if self._is_config_complete(cfg):
                    self.mc_button.config(state="normal")
                    print("✓ Monte Carlo button ENABLED")
                else:
                    self.mc_button.config(state="disabled")
                    print("✗ Monte Carlo button DISABLED - not enough weights configured")
            except Exception as e:
                self.mc_button.config(state="disabled")
                print(f"✗ Monte Carlo button DISABLED - error: {e}")

    def _auto_load_config(self):
        """Lädt bestehende Konfiguration beim Start, falls vorhanden."""
        config_path = Path("Output/MCA/Weights_MC/kpi_config.json")

        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                self._load_config_into_gui(data)
                print(f"✓ Configuration loaded from: {config_path}")
                self._update_monte_carlo_button()

            except Exception as e:
                import traceback
                error_msg = f"Failed to auto-load config: {e}\n\n{traceback.format_exc()}"
                print(error_msg)
                messagebox.showerror("Auto-Load Error",
                                     f"Could not load configuration:\n{str(e)}\n\nStarting with empty config.")

    def _load_config_into_gui(self, data: dict):
        """Schreibt Konfigurationswerte in GUI-Widgets zurück."""
        try:
            if 'excel_path' in data:
                self.var_excel.set(data['excel_path'])
            if 'id_columns' in data:
                self.var_ids.set(", ".join(data['id_columns']))

            if 'winsorize_pct' in data and data['winsorize_pct'] is not None:
                self.var_wins.set(float(data['winsorize_pct']))
            if 'imputation_enabled' in data:
                self.var_imp.set(bool(data['imputation_enabled']))
            if 'imputation_method' in data:
                self.var_imp_method.set(data['imputation_method'])

            # Risk-Thresholds laden
            if 'risk_thresholds' in data:
                rt = data['risk_thresholds']
                for risk_key in self.risk_vars.keys():
                    if risk_key in rt:
                        threshold_data = rt[risk_key]
                        if risk_key == "area":
                            if 'sufficient' in threshold_data:
                                self.risk_vars[risk_key]["safe"].set(str(int(threshold_data['sufficient'])))
                            if 'critical' in threshold_data:
                                self.risk_vars[risk_key]["critical"].set(str(int(threshold_data['critical'])))
                        else:
                            if 'safe' in threshold_data:
                                self.risk_vars[risk_key]["safe"].set(str(int(threshold_data['safe'])))
                            if 'critical' in threshold_data:
                                self.risk_vars[risk_key]["critical"].set(str(int(threshold_data['critical'])))

            if 'categories' in data:
                self._load_categories_into_gui(data['categories'])

        except Exception as e:
            print(f"Error loading config into GUI: {e}")

    def _load_categories_into_gui(self, categories_data: dict):
        """Lädt Kategorien (KPIs + Kategoriegewichte) in die GUI."""
        try:
            for cat_name, cat_data in categories_data.items():
                self._load_kpis_into_category(cat_name, cat_data.get('kpis', []))

                if cat_name in self.category_weight_vars:
                    cat_vars = self.category_weight_vars[cat_name]
                    if 'w_min' in cat_data and cat_data['w_min'] is not None:
                        cat_vars['w_min'].set(str(cat_data['w_min']))
                    if 'w_max' in cat_data and cat_data['w_max'] is not None:
                        cat_vars['w_max'].set(str(cat_data['w_max']))

        except Exception as e:
            print(f"Error loading categories: {e}")

    def _load_kpis_into_category(self, cat_key: str, kpis_data: list):
        """Lädt KPI-Min/Max-Gewichte in bestehende KPI-Variablen."""
        try:
            if cat_key not in self.kpi_vars:
                return

            cat_data = self.kpi_vars[cat_key]

            for kpi_vars in cat_data["kpis"]:
                kpi_name = kpi_vars["name"]

                for kpi_data in kpis_data:
                    if kpi_data.get('name') == kpi_name:
                        if kpi_data.get('w_min') is not None:
                            kpi_vars["w_min"].set(str(kpi_data['w_min']))
                        if kpi_data.get('w_max') is not None:
                            kpi_vars["w_max"].set(str(kpi_data['w_max']))
                        break

        except Exception as e:
            print(f"Error loading KPIs into category {cat_key}: {e}")

    def _show_silent_info(self, title: str, message: str):
        """Info-Dialog ohne System-Sound."""
        dialog = tk.Toplevel(self)
        dialog.title(title)
        dialog.geometry("450x250")
        dialog.resizable(False, False)
        dialog.transient(self)
        dialog.grab_set()

        # Dialog zentrieren
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (450 // 2)
        y = (dialog.winfo_screenheight() // 2) - (250 // 2)
        dialog.geometry(f"450x250+{x}+{y}")

        # Inhalt
        main_frame = ttk.Frame(dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)

        icon_frame = ttk.Frame(main_frame)
        icon_frame.pack(fill=tk.X, pady=(0, 15))

        ttk.Label(icon_frame, text="ℹ️", font=("Arial", 16)).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Label(icon_frame, text=title, font=("Arial", 12, "bold")).pack(side=tk.LEFT)

        text_widget = tk.Text(main_frame, height=5, wrap=tk.WORD, font=("Arial", 9))
        text_widget.pack(fill=tk.BOTH, expand=True, pady=(0, 20))
        text_widget.insert("1.0", message)
        text_widget.config(state=tk.DISABLED)

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)

        ok_button = ttk.Button(button_frame, text="OK", command=dialog.destroy)
        ok_button.pack(pady=10)

        ok_button.focus_set()
        dialog.bind('<Return>', lambda e: dialog.destroy())
        dialog.bind('<Escape>', lambda e: dialog.destroy())

        dialog.wait_window()

    def _start_monte_carlo(self):
        """Startet das Monte-Carlo-Parameterfenster."""
        if not MONTE_CARLO_AVAILABLE:
            messagebox.showerror("Error", "Monte Carlo extension is not available")
            return

        # Config-Datei muss existieren
        config_path = Path("Output/MCA/Weights_MC/kpi_config.json")
        if not config_path.exists():
            messagebox.showwarning("Warning",
                                   "Please export JSON configuration first before starting Monte Carlo analysis")
            return

        # Plant-Database muss existieren
        db_path = Path(self.var_excel.get().strip() or "Output/UWWTD_TP_Database.xlsx")
        if not db_path.exists():
            messagebox.showerror("Error",
                                 f"Plant database not found: {db_path}\n\n"
                                 "Please ensure the UWWTD database file exists before running Monte Carlo analysis.")
            return

        try:
            mc_window = MonteCarloWindow(self, str(config_path))
            mc_window.transient(self)
            mc_window.grab_set()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to start Monte Carlo analysis: {str(e)}")

    def _back_to_overview(self):
        """Kehrt zum Launcher zurück."""
        try:
            import subprocess
            from pathlib import Path

            self.destroy()

            launcher_path = Path(__file__).parent / "01_launcher.py"
            if launcher_path.exists():
                subprocess.Popen([sys.executable, str(launcher_path)], cwd=str(launcher_path.parent))
        except Exception as e:
            print(f"Error returning to launcher: {e}")
            self.destroy()


if __name__ == "__main__":
    App().mainloop()


