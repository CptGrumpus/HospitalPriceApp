import subprocess
import sys
import os

def run_script(script_name, args=[]):
    """Runs a python script as a subprocess."""
    cmd = [sys.executable, script_name] + args
    print(f"\n>>> Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True)
        if result.returncode == 0:
            print(f">>> {script_name} completed successfully.")
        else:
            print(f">>> {script_name} failed with code {result.returncode}.")
            return False
    except subprocess.CalledProcessError as e:
        print(f">>> Error running {script_name}: {e}")
        return False
    return True

def main():
    print("=== STARTING FULL INGESTION PIPELINE ===")
    
    # 1. Reset Database
    db_file = "hospital.db"
    if os.path.exists(db_file):
        print(f"Removing existing database: {db_file}")
        os.remove(db_file)
    
    # 2. Ingest HCPCS Definitions
    # The script expects the file path as an argument or uses a default
    # We'll assume the default path inside the script works, or pass it explicitly if needed.
    if not run_script("scripts/ingest_hcpcs_definitions.py"):
        print("Pipeline stopped due to error.")
        return

    # 3. Ingest Beaumont (Tall)
    if not run_script("scripts/ingest_tall.py", ["data/raw/beaumontroyaloak.csv", "BEAUMONT"]):
        print("Pipeline stopped due to error.")
        return

    # 4. Ingest Children's (Tall)
    if not run_script("scripts/ingest_tall.py", ["data/raw/childrenshospitalofmichigan_detroit.csv", "CHILDRENS"]):
        print("Pipeline stopped due to error.")
        return

    # 5. Ingest UofM (Wide)
    if not run_script("scripts/ingest_wide.py", ["data/raw/universityofmichigan.csv", "UOFM"]):
        print("Pipeline stopped due to error.")
        return

    # 6. Ingest Henry Ford (Tall)
    if not run_script("scripts/ingest_tall.py", ["data/raw/henryforddetroit.csv", "HENRYFORD"]):
        print("Pipeline stopped due to error.")
        return

    print("\n=== ALL INGESTION TASKS COMPLETED ===")

if __name__ == "__main__":
    main()

