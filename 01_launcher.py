


import sys
import subprocess
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path


class MCLauncher(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Multi Criteria Analysis - Launcher")
        self.geometry("650x450")  # Für 2 Buttons
        self.resizable(False, False)
        
        # Styling
        self.configure(bg="white")
        
        self._build_ui()
        self._center_on_screen()
    
    def _build_ui(self):
        """UI aufbauen"""
        # Main Container
        main_frame = tk.Frame(self, bg="white", padx=40, pady=30)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Titel
        title_label = tk.Label(
            main_frame,
            text="Multi Criteria Analysis",
            font=("Segoe UI", 18, "bold"),
            bg="white",
            fg="#111111"
        )
        title_label.pack(pady=(0, 10))
        
        # Untertitel
        subtitle_label = tk.Label(
            main_frame,
            text="Choose your action",
            font=("Segoe UI", 11),
            bg="white",
            fg="#666666"
        )
        subtitle_label.pack(pady=(0, 40))
        
        # Button-Container
        button_frame = tk.Frame(main_frame, bg="white")
        button_frame.pack(fill=tk.BOTH, expand=True)
        
        # Button 1: Zur Multi Criteria Analysis
        self._create_option_button(
            button_frame,
            title="Go to weight configuration",
            description="Continue with Monte Carlo simulation",
            command=self._open_mca,
            row=0
        )
        
        # Trennlinie
        separator = tk.Frame(button_frame, bg="#e6e6e6", height=1)
        separator.pack(fill=tk.X, pady=20)
        
        # Button 2: Ranking anschauen
        self._create_option_button(
            button_frame,
            title="View Main Ranking",
            description="Visualize complete ranking results (Calculated Values)",
            command=self._open_view_ranking,
            row=1
        )
        
        # Footer
        footer_frame = tk.Frame(main_frame, bg="white")
        footer_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(30, 0))
        
        footer_label = tk.Label(
            footer_frame,
            text="Leibniz University Hanover  |  IfES",
            font=("Segoe UI", 9),
            bg="white",
            fg="#999999"
        )
        footer_label.pack(side=tk.RIGHT)
        
        # ESC zum Schließen
        self.bind("<Escape>", lambda e: self.destroy())
    
    def _create_option_button(self, parent, title, description, command, row):
        """Erstellt einen schicken Button mit Hover-Effekt"""
        # Container mit Rahmen
        container = tk.Frame(parent, bg="#e6e6e6")
        container.pack(fill=tk.X, pady=5)
        
        # Innerer Button-Bereich
        btn_frame = tk.Frame(container, bg="white", cursor="hand2")
        btn_frame.pack(fill=tk.BOTH, expand=True, padx=1, pady=1)
        
        # Titel
        title_label = tk.Label(
            btn_frame,
            text=title,
            font=("Segoe UI", 12, "bold"),
            bg="white",
            fg="#111111",
            anchor="w"
        )
        title_label.pack(fill=tk.X, padx=20, pady=(15, 5))
        
        # Beschreibung
        desc_label = tk.Label(
            btn_frame,
            text=description,
            font=("Segoe UI", 10),
            bg="white",
            fg="#666666",
            anchor="w"
        )
        desc_label.pack(fill=tk.X, padx=20, pady=(0, 15))
        
        # Hover-Effekte (wird heller wenn man drüber geht)
        def on_enter(e):
            btn_frame.configure(bg="#f5f5f5")
            title_label.configure(bg="#f5f5f5")
            desc_label.configure(bg="#f5f5f5")
        
        def on_leave(e):
            btn_frame.configure(bg="white")
            title_label.configure(bg="white")
            desc_label.configure(bg="white")
        
        # Klick und Hover an alle Widgets binden
        for widget in [btn_frame, title_label, desc_label]:
            widget.bind("<Button-1>", lambda e: command())
            widget.bind("<Enter>", on_enter)
            widget.bind("<Leave>", on_leave)
    
    def _center_on_screen(self):
        """Zentriert das Fenster auf dem Bildschirm"""
        self.update_idletasks()
        w = 650
        h = 450
        sw = self.winfo_screenwidth()
        sh = self.winfo_screenheight()
        x = (sw - w) // 2
        y = (sh - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")
    
    def _open_mca(self):
        """Öffnet die Multi Criteria Analysis (Gewichte & Monte Carlo)"""
        try:
            script_path = Path(__file__).parent / "01_potential_ranking.py"
            if not script_path.exists():
                messagebox.showerror("Error", f"Script not found:\n{script_path}")
                return
            
            # Launcher-Fenster verstecken
            self.withdraw()
            
            # Als separaten Prozess starten
            if sys.platform.startswith('win'):
                # CREATE_NEW_PROCESS_GROUP damit das Fenster im Vordergrund erscheint
                subprocess.Popen(
                    [sys.executable, str(script_path)], 
                    cwd=str(script_path.parent),
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                )
            else:
                subprocess.Popen([sys.executable, str(script_path)], cwd=str(script_path.parent))
            
            # Launcher nach kurzer Zeit schließen
            self.after(200, self.destroy)
        except Exception as e:
            import traceback
            print(f"Error opening MCA: {e}")
            traceback.print_exc()
            self.deiconify()  # Launcher wieder anzeigen falls Fehler
            messagebox.showerror("Error", f"Failed to open Multi Criteria Analysis:\n{str(e)}")
    
    def _open_view_ranking(self):
        """Öffnet den Ranking-Viewer (Ergebnisse anschauen)"""
        try:
            script_path = Path(__file__).parent / "01_view_ranking.py"
            if not script_path.exists():
                messagebox.showerror("Error", f"Script not found:\n{script_path}")
                return
            

            self.withdraw()
            

            if sys.platform.startswith('win'):

                subprocess.Popen(
                    [sys.executable, str(script_path)], 
                    cwd=str(script_path.parent),
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                )
            else:
                subprocess.Popen([sys.executable, str(script_path)], cwd=str(script_path.parent))
            

            self.after(200, self.destroy)
        except Exception as e:
            import traceback
            print(f"Error opening View Ranking: {e}")
            traceback.print_exc()
            self.deiconify()
            messagebox.showerror("Error", f"Failed to open View Ranking:\n{str(e)}")


if __name__ == "__main__":
    MCLauncher().mainloop()
