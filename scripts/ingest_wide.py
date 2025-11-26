import pandas as pd
import sys
import os

# Add project root to python path
sys.path.append(os.getcwd())

from sqlalchemy.orm import Session
from src.database import SessionLocal, Item, Price, init_db

# Helper function to parse currency
def parse_price(price_str):
    """
    Returns tuple: (price_value, notes_str)
    """
    if pd.isna(price_str) or price_str == '':
        return None, None
        
    price_str_clean = str(price_str).strip()
    
    # Heuristic for Formulas
    if 'Formula' in price_str_clean or 'algorithm' in price_str_clean.lower():
        return None, price_str_clean

    try:
        # Remove $ and , and convert to float
        clean = price_str_clean.replace('$', '').replace(',', '')
        val = float(clean)
        
        # For ingest_wide (UofM), check for placeholders if they exist (though UofM data looked cleaner)
        # But for consistency, let's apply the same 99999999 check
        if val >= 99999999:
            return None, None
            
        return val, None
    except:
        # Return original string as note if parse fails
        return None, price_str_clean

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
            # 1. Smart Code Extraction
            # We want to prioritize standard codes (CPT, HCPCS, DRG) over internal CDM codes
            
            final_code = row.get('code|1', 'UNKNOWN')
            final_type = row.get('code|1|type', 'UNKNOWN')
            
            # Scan columns 1 through 5 (assumed max) for a better code
            # We prioritize: CPT > HCPCS > MS-DRG > APR-DRG > NDC > CDM
            
            priority_map = {
                'CPT': 1,
                'HCPCS': 2,
                'MS-DRG': 3,
                'APR-DRG': 4,
                'NDC': 5,
                'CDM': 99,
                'Local': 99,
                'RC': 99,
                'UNKNOWN': 100
            }
            
            current_priority = priority_map.get(final_type, 100)
            
            for i in range(1, 6):
                code_col = f'code|{i}'
                type_col = f'code|{i}|type'
                
                if code_col in row and type_col in row:
                    this_code = row[code_col]
                    this_type = row[type_col]
                    
                    if pd.isna(this_code) or pd.isna(this_type):
                        continue
                        
                    this_prio = priority_map.get(this_type, 100)
                    
                    # If this code is higher priority (lower number), swap it in
                    if this_prio < current_priority:
                        final_code = this_code
                        final_type = this_type
                        current_priority = this_prio
            
            # NORMALIZE: Force CPT vs HCPCS based on format
            # CPT: 5 digits (numeric)
            # HCPCS: Letter + 4 digits (or similar)
            if len(str(final_code).strip()) == 5:
                if str(final_code).isdigit():
                    final_type = 'CPT'
                elif str(final_code)[0].isalpha():
                    final_type = 'HCPCS'

            # -------------------------------------------------------
            
            desc = row.get('description', 'No Description')
            setting = row.get('billing_class', 'UNKNOWN')
            
            item = Item(
                code=final_code, 
                code_type=final_type,
                description=desc, 
                hospital_id=hospital_id, 
                setting=setting
            )
            session.add(item)
            session.flush() # Get the ID of the new item

            # 2. Extract Prices (The "Melting" Phase)
            # We look for columns that indicate a price.
            # Strategy: If it contains "standard_charge", we parse it.
            
            for col in df.columns:
                # TODO: Make this filter customizable for other hospitals
                if 'standard_charge' in col:
                    price_val, price_note = parse_price(row[col])
                    
                    if price_val is None and price_note is None:
                         # Sibling check for UofM
                         # UofM has 'negotiated_algorithm', 'negotiated_percentage', 'methodology'
                         potential_suffixes = ['negotiated_algorithm', 'methodology', 'negotiated_percentage']
                         base_col = col
                         
                         for suffix in potential_suffixes:
                             # Try replacing last part
                             parts = base_col.split('|')
                             # UofM keys usually end in 'negotiated_dollar' or similar
                             if parts[-1] in ['negotiated_dollar', 'estimated_amount']:
                                 parts[-1] = suffix
                                 sibling_col = "|".join(parts)
                                 if sibling_col in row and not pd.isna(row[sibling_col]):
                                     sibling_val = str(row[sibling_col]).strip()
                                     if sibling_val and sibling_val != '':
                                         price_note = f"{suffix}: {sibling_val}"
                                         break
                    
                    if price_val is not None or price_note is not None:
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
                            amount=price_val,
                            notes=price_note
                        )
                        session.add(price_entry)

            count += 1
            if count % 1000 == 0:
                print(f"Processed {count} rows...")
                session.commit() # Commit every 1000 rows
                
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
