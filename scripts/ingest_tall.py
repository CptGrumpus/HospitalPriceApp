import pandas as pd
import sys
import os
from sqlalchemy.orm import Session
from src.database import SessionLocal, Item, Price, init_db

def parse_price(price_str):
    if pd.isna(price_str) or price_str == '':
        return None
    try:
        clean = str(price_str).replace('$', '').replace(',', '').strip()
        return float(clean)
    except:
        return None

def ingest_tall_csv(file_path, hospital_id="BEAUMONT"):
    print(f"--- Starting Tall CSV Ingestion for: {file_path} (Hospital: {hospital_id}) ---")
    
    init_db()
    session = SessionLocal()

    try:
        print("Reading CSV... (this may take a moment)")
        # Beaumont uses header on row 3 (index 2)
        try:
            df = pd.read_csv(file_path, header=2, dtype=str, encoding='utf-8')
        except UnicodeDecodeError:
            print("UTF-8 failed. Trying ISO-8859-1...")
            df = pd.read_csv(file_path, header=2, dtype=str, encoding='iso-8859-1')
        
        print(f"Loaded {len(df)} rows. Processing...")

        # Cache to avoid creating duplicate items for every payer row
        # Key: (code, description, setting) -> item_id
        item_cache = {}
        
        count = 0
        
        for index, row in df.iterrows():
            code = row.get('code|1', 'UNKNOWN')
            code_type = row.get('code|1|type', 'UNKNOWN')
            desc = row.get('description', 'No Description')
            
            # Prefer 'setting' column (e.g. 'outpatient'), fallback to 'billing_class'
            setting = row.get('setting')
            if pd.isna(setting) or setting == '':
                setting = row.get('billing_class', 'UNKNOWN')
            
            # 1. Resolve Item (Get ID or Create New)
            item_key = (code, desc, setting)
            
            if item_key in item_cache:
                item_id = item_cache[item_key]
            else:
                # Create new item
                new_item = Item(
                    code=code, 
                    code_type=code_type,
                    description=desc, 
                    hospital_id=hospital_id, 
                    setting=setting
                )
                session.add(new_item)
                session.flush() # Get the ID
                item_id = new_item.id
                item_cache[item_key] = item_id

            # 2. Create Price
            # Tall CSV has payer/plan in columns
            payer = row.get('payer_name')
            plan = row.get('plan_name', None)
            
            # A. Negotiated Price (If Payer exists)
            if not pd.isna(payer) and payer != '':
                price_str = row.get('standard_charge|negotiated_dollar')
                if pd.isna(price_str) or price_str == '':
                    price_str = row.get('estimated_amount')
                
                price_val = parse_price(price_str)
                if price_val is not None:
                    session.add(Price(item_id=item_id, payer=payer, plan=plan, amount=price_val))

            # B. Gross / Cash Prices
            # Capture location/notes from the last column to distinguish duplicates
            # Example: "Gross Charge Type: F Fh Hosp Based Clinics Facility Charges"
            notes = row.get('additional_generic_notes')
            location_info = None
            if not pd.isna(notes):
                # Clean up the note
                location_info = str(notes).replace('Gross Charge Type:', '').strip()

            gross_str = row.get('standard_charge|gross')
            gross_val = parse_price(gross_str)
            if gross_val is not None:
                 session.add(Price(item_id=item_id, payer="GROSS", plan=location_info, amount=gross_val))

            cash_str = row.get('standard_charge|discounted_cash')
            cash_val = parse_price(cash_str)
            if cash_val is not None:
                 session.add(Price(item_id=item_id, payer="DISCOUNTED_CASH", plan=location_info, amount=cash_val))
            
            count += 1
            if count % 1000 == 0:
                print(f"Processed {count} rows...")
                session.commit()

        session.commit()
        print("--- Ingestion Complete ---")

    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        default_path = "data/raw/beaumont.csv"
        if os.path.exists(default_path):
            ingest_tall_csv(default_path)
        else:
            print("Usage: python3 scripts/ingest_tall.py <CSV_PATH>")
    else:
        ingest_tall_csv(sys.argv[1])
