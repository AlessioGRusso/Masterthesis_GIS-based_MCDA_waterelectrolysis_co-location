 nbimport subprocess
import time
import sys
from datetime import datetime

def main():
    script_name = "40_renewable_energy_profiles.py"
    wait_minutes = 61
    
    print(f"=== Renewables Auto-Restart Script ===")
    print(f"Script: {script_name}")
    print(f"Intervall: {wait_minutes} Minuten")
    print(f"Drücke Ctrl+C zum Beenden\n")
    
    run_count = 0
    
    while True:
        run_count += 1
        
        # Script starten
        print(f"\n{'='*70}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Durchlauf #{run_count} - Starte {script_name}")
        print(f"{'='*70}\n")
        
        try:
            result = subprocess.call([sys.executable, script_name])
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Script beendet (Exit Code: {result})")
        except Exception as e:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Fehler: {e}")
        
        # Warten
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Warte {wait_minutes} Minuten...")
        
        try:
            for i in range(wait_minutes):
                remaining = wait_minutes - i
                print(f"  Noch {remaining} Minuten bis zum nächsten Start...", end='\r')
                time.sleep(60)
            print()  # Neue Zeile
        except KeyboardInterrupt:
            print(f"\n\n[{datetime.now().strftime('%H:%M:%S')}] Programm beendet.")
            sys.exit(0)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n[{datetime.now().strftime('%H:%M:%S')}] Programm beendet.")
        sys.exit(0)
