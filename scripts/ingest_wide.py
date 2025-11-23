import pandas as pd
import sys
import os
from sqlalchemy.orm import Session
from src.database import SessionLocal, Item, Price, init_db

# Helper function to parse currency
def parse_price(price_str):
    if pd.isna(price_str) or price_str == '':
        return None
    try:
        # Remove $ and , and convert to float
        clean = str(price_str).replace('$', '').replace(',', '').strip()
        return float(clean)
    except:
        return None

def ingest_wide_csv(file_path, hospital_id="UNKNOWN"):
    print(f"--- Starting Wide CSV Ingestion for: {file_path} (Hospital: {hospital_id}) ---")
    
    # Initialize DB tables
    init_db()
    
    # Create a DB session
    session = SessionLocal()

    try:
        # Read the CSV
        # NOTE: This header=2 is specific to UofM's file structure (skipping first 2 lines).
        # In a fully generic script, we might detect this or pass it as an arg.
        print("Reading CSV... (this may take a moment)")
        try:
            df = pd.read_csv(file_path, header=2, dtype=str, encoding='utf-8')
        except UnicodeDecodeError:
            print("UTF-8 failed. Trying ISO-8859-1...")
            df = pd.read_csv(file_path, header=2, dtype=str, encoding='iso-8859-1')
        
        print(f"Loaded {len(df)} rows. Processing...")

        # Iterate through rows
        count = 0
        for index, row in df.iterrows():
            # 1. Create the Item
            # Note: Column names 'code' and 'description' are assumed standard for now.
            # UofM uses "code|1" as the primary code.
            code = row.get('code|1', row.get('code', 'UNKNOWN'))
            desc = row.get('description', 'No Description')
            setting = row.get('billing_class', 'UNKNOWN')
            
            item = Item(code=code, description=desc, hospital_id=hospital_id, setting=setting)
            session.add(item)
            session.flush() # Get the ID of the new item

            # 2. Extract Prices (The "Melting" Phase)
            # We look for columns that indicate a price.
            # Strategy: If it contains "standard_charge", we parse it.
            
            for col in df.columns:
                # TODO: Make this filter customizable for other hospitals
                if 'standard_charge' in col:
                    price_val = parse_price(row[col])
                    
                    if price_val is not None:
                        # Parse the column name to get Payer and Plan
                        # UofM Format: "standard_charge|Payer Name|Plan Name|negotiated_dollar"
                        parts = col.split('|')
                        
                        payer_name = "Unknown"
                        plan_name = None

                        if len(parts) > 1:
                            if parts[1] in ['gross', 'discounted_cash', 'min', 'max']:
                                payer_name = parts[1].upper() # GROSS, CASH, MIN, MAX
                                plan_name = "Standard"
                            else:
                                payer_name = parts[1] # The Insurance Company Name
                                if len(parts) > 2:
                                    plan_name = parts[2] # The Plan Name (HMO, PPO, etc.)

                        # Create Price entry
                        price_entry = Price(
                            item_id=item.id,
                            payer=payer_name,
                            plan=plan_name,
                            amount=price_val
                        )
                        session.add(price_entry)

            count += 1
            if count % 1000 == 0:
                print(f"Processed {count} rows...")
                session.commit() # Commit every 1000 rows
                
            # DEBUG LIMIT REMOVED for full ingestion
            # if count >= 500:
            #     print("--- DEBUG LIMIT REACHED (500 rows) ---")
            #     break

        session.commit()
        print("--- Ingestion Complete ---")

    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    # Default usage
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/ingest_wide.py <CSV_PATH> [HOSPITAL_ID]")
        # Fallback for local testing if just running blindly
        default_path = "data/raw/38-6006309_UNIVERSITY-OF-MICHIGAN-HEALTH_standardcharges.csv"
        if os.path.exists(default_path):
            print(f"No arguments provided. Using default: {default_path}")
            ingest_wide_csv(default_path, "UOFM")
        else:
            sys.exit(1)
    else:
        csv_path = sys.argv[1]
        hosp_id = sys.argv[2] if len(sys.argv) > 2 else "UNKNOWN"
        
        if not os.path.exists(csv_path):
            print(f"File not found: {csv_path}")
        else:
            ingest_wide_csv(csv_path, hosp_id)
