import sys
import subprocess
from pathlib import Path
import tkinter as tk
from tkinter import messagebox

# Basisverzeichnis des Projekts
BASE_DIR = Path(__file__).resolve().parent

# Launcher-Ziele
SCRIPTS = [
    ("Database", "00_database.py", "Datenbank verwalten und konfigurieren"),
    ("Multi Criteria Analysis", "01_launcher.py", "Gewichte einstellen und Monte Carlo Simulation starten"),
]

# Texte
TITLE = "Masterthesis (M. Sc.) 2025"
SUBTITLE = "Alessio Russo  •  Supervisor: Levin Matz"
FOOTER = "Leibniz University Hanover  |  IfES"

# UI-Grundfarben & Fonts (bewusst simpel gehalten)
BG = "white"
FG = "#111"
SUB_FG = "#666"
BORDER = "#e6e6e6"
HOVER = "#f5f5f5"

FONT_TITLE = ("Segoe UI", 18, "bold")
FONT_SUB = ("Segoe UI", 11)
FONT_BTN_TITLE = ("Segoe UI", 12, "bold")
FONT_BTN_DESC = ("Segoe UI", 10)
FONT_FOOTER = ("Segoe UI", 9)


def _python_no_console() -> str:
    """
    Unter Windows: versucht pythonw.exe zu nutzen,
    damit GUI-Skripte ohne zusätzliches Konsolenfenster starten.
    """
    exe = Path(sys.executable)
    if sys.platform.startswith("win") and exe.name.lower() == "python.exe":
        pythonw = exe.with_name("pythonw.exe")
        if pythonw.exists():
            return str(pythonw)
    return sys.executable


def start_script(filename: str, root: tk.Tk) -> None:
    """Startet ein Subskript als eigenen Prozess."""
    path = BASE_DIR / filename
    if not path.exists():
        messagebox.showerror("Fehler", f"Datei nicht gefunden:\n{path}")
        return

    try:
        # Einige Module sollen bewusst mit Konsole laufen
        gui_scripts = {
            "00_database.py",
            "01_potential_grading.py",
            "02_potential_analysis.py",
        }

        use_console = filename in gui_scripts

        if use_console:
            cmd = [sys.executable, str(path)]
            flags = 0
        else:
            cmd = [_python_no_console(), str(path)]
            flags = 0x08000000 if sys.platform.startswith("win") else 0

        subprocess.Popen(cmd, cwd=str(BASE_DIR), creationflags=flags)

    except Exception as e:
        messagebox.showerror("Fehler beim Starten", str(e))


def add_button(parent, title, description, file):
    """Erzeugt einen klickbaren Launcher-Button."""
    container = tk.Frame(parent, bg=BORDER)

    btn = tk.Frame(container, bg=BG, cursor="hand2")
    btn.pack(fill="both", expand=True, padx=1, pady=1)

    lbl_title = tk.Label(btn, text=title, font=FONT_BTN_TITLE, bg=BG, fg=FG, anchor="w")
    lbl_title.pack(fill="x", padx=20, pady=(15, 5))

    lbl_desc = tk.Label(btn, text=description, font=FONT_BTN_DESC, bg=BG, fg=SUB_FG, anchor="w")
    lbl_desc.pack(fill="x", padx=20, pady=(0, 15))

    def hover(on: bool):
        color = HOVER if on else BG
        btn.configure(bg=color)
        lbl_title.configure(bg=color)
        lbl_desc.configure(bg=color)

    for w in (btn, lbl_title, lbl_desc):
        w.bind("<Button-1>", lambda _, f=file: start_script(f, parent.winfo_toplevel()))
        w.bind("<Enter>", lambda _: hover(True))
        w.bind("<Leave>", lambda _: hover(False))

    return container


def center_on_screen(win: tk.Tk):
    """Fenster mittig auf dem Bildschirm platzieren."""
    win.update_idletasks()
    w, h = win.winfo_width(), win.winfo_height()
    sw, sh = win.winfo_screenwidth(), win.winfo_screenheight()
    win.geometry(f"{w}x{h}+{(sw - w) // 2}+{(sh - h) // 2}")


def main():
    root = tk.Tk()
    root.title("Masterthesis Launcher")
    root.configure(bg=BG)
    root.geometry("650x400")
    root.resizable(False, False)

    main_frame = tk.Frame(root, bg=BG, padx=40, pady=30)
    main_frame.pack(fill="both", expand=True)

    tk.Label(main_frame, text=TITLE, font=FONT_TITLE, bg=BG, fg=FG).pack(pady=(0, 10))
    tk.Label(main_frame, text=SUBTITLE, font=FONT_SUB, bg=BG, fg=SUB_FG).pack(pady=(0, 40))

    btn_frame = tk.Frame(main_frame, bg=BG)
    btn_frame.pack(fill="both", expand=True)

    for i, (title, file, desc) in enumerate(SCRIPTS):
        add_button(btn_frame, title, desc, file).pack(fill="x", pady=5)
        if i < len(SCRIPTS) - 1:
            tk.Frame(btn_frame, bg=BORDER, height=1).pack(fill="x", pady=20)

    footer = tk.Label(main_frame, text=FOOTER, font=FONT_FOOTER, bg=BG, fg="#999")
    footer.pack(side="bottom", anchor="e", pady=(30, 0))

    root.bind("<Escape>", lambda _: root.destroy())
    center_on_screen(root)
    root.mainloop()


if __name__ == "__main__":
    main()

