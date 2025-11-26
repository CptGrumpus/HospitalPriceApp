import sys
import os

# Add project root to python path
sys.path.append(os.getcwd())

import pandas as pd
from sqlalchemy.orm import Session
from src.database import SessionLocal, Item, Price, init_db

def parse_price(price_str):
    """
    Returns tuple: (price_value, notes_str)
    """
    if pd.isna(price_str) or price_str == '' or str(price_str).strip() == '':
        return None, None

    price_str_clean = str(price_str).strip()

    # CHECK 1: Text Formula (starts with Formula or contains alphabetical chars beyond reasonable typos)
    # Heuristic: If it contains 'Formula', we capture it as a note.
    if 'Formula' in price_str_clean or 'algorithm' in price_str_clean.lower():
        return None, price_str_clean

    try:
        # Remove $ and , and convert to float
        clean = price_str_clean.replace('$', '').replace(',', '')
        val = float(clean)
        
        # FILTER: Ignore placeholder prices often found in Children's data
        if val >= 99999999:
            # Return None for price, but we might use this signal to look for sibling columns later
            return None, None
            
        return val, None
    except:
        # If it fails to parse as float, treat as note string if non-empty
        return None, price_str_clean

def ingest_tall_csv(file_path, hospital_id="BEAUMONT"):
    print(f"--- Starting Tall CSV Ingestion for: {file_path} (Hospital: {hospital_id}) ---")
    
    init_db()
    session = SessionLocal()

    try:
        print("Reading CSV... (this may take a moment)")
        # 1. Auto-Detect Header Row
        # Beaumont uses header=2 (Row 3)
        # Children's uses header=2 (Row 3)
        # So we can try header=2 first. If columns look wrong, maybe try others?
        # For now, both seem to be header=2.
        
        try:
            df = pd.read_csv(file_path, header=2, dtype=str, encoding='utf-8')
        except UnicodeDecodeError:
            print("UTF-8 failed. Trying ISO-8859-1...")
            df = pd.read_csv(file_path, header=2, dtype=str, encoding='iso-8859-1')
            
        # Verify we have expected columns
        if 'code|1' not in df.columns and 'description' not in df.columns:
            print("WARNING: Header detection might be wrong. Columns found:", df.columns[:5])
        
        print(f"Loaded {len(df)} rows. Processing...")

        # Cache to avoid creating duplicate items for every payer row
        # Key: (code, description, setting) -> item_id
        item_cache = {}
        
        count = 0
        
        for index, row in df.iterrows():
            # 1. Smart Code Extraction (Standard Logic)
            final_code = row.get('code|1', 'UNKNOWN')
            final_type = row.get('code|1|type', 'UNKNOWN')
            
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

                    # VALIDATION: Ignore bogus HCPCS/CPT codes
                    if this_type in ['CPT', 'HCPCS']:
                        if len(str(this_code).strip()) != 5:
                            this_type = 'Local'
                        
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

            # -----------------------------------------------------

            desc = row.get('description', 'No Description')
            
            # Prefer 'setting' column (e.g. 'outpatient'), fallback to 'billing_class'
            setting = row.get('setting')
            if pd.isna(setting) or setting == '':
                setting = row.get('billing_class', 'UNKNOWN')
            
            # 1. Resolve Item (Get ID or Create New)
            item_key = (final_code, desc, setting)
            
            if item_key in item_cache:
                item_id = item_cache[item_key]
            else:
                # Create new item
                new_item = Item(
                    code=final_code, 
                    code_type=final_type,
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
            
            # Strategy: Look for specific columns.
            # Beaumont uses: 'payer_name', 'plan_name', 'standard_charge|negotiated_dollar'
            # Children's uses: 'standard_charge|Payer|Plan|negotiated_dollar'
            
            # Branch A: If 'payer_name' column exists (Beaumont Style)
            if 'payer_name' in row and not pd.isna(row['payer_name']):
                payer = row.get('payer_name')
                plan = row.get('plan_name', None)
                
                price_str = row.get('standard_charge|negotiated_dollar')
                if pd.isna(price_str) or price_str == '':
                    price_str = row.get('estimated_amount')
                
                price_val, price_note = parse_price(price_str)
                
                # Sibling extraction for Beaumont (Tall Format)
                if price_val is None and (price_note is None or "Placeholder" in str(price_note)):
                    algo_col = 'standard_charge|negotiated_algorithm'
                    if algo_col in row and not pd.isna(row[algo_col]):
                        algo_val = str(row[algo_col]).strip()
                        if algo_val and algo_val != '':
                            price_note = f"Algorithm: {algo_val}"
                            
                if price_val is not None or price_note is not None:
                    session.add(Price(item_id=item_id, payer=payer, plan=plan, amount=price_val, notes=price_note))

            # Branch B: If columns define payers (Children's Style)
            # We scan columns for 'negotiated_dollar' or 'estimated_amount'
            else:
                # Children's Format: "standard_charge|Payer Name|Plan Name|negotiated_dollar"
                for col in df.columns:
                    # Check for dollar columns OR algorithm/methodology columns
                    is_dollar_col = 'negotiated_dollar' in col or 'estimated_amount' in col
                    
                    # Also check for corresponding 'negotiated_algorithm' column if price is empty?
                    # Actually, 'negotiated_dollar' column itself sometimes contains the "Formula..." string in this dataset.
                    
                    if is_dollar_col:
                        price_val, price_note = parse_price(row[col])
                        
                        # If we didn't get a price, maybe check the 'negotiated_algorithm' sibling column?
                        # Sibling extraction logic:
                        if price_val is None and (price_note is None or "Placeholder" in str(price_note)):
                             # Construct potential sibling column names
                             # e.g. replace 'negotiated_dollar' with 'negotiated_algorithm' or 'methodology'
                             potential_suffixes = ['negotiated_algorithm', 'methodology', 'negotiated_percentage']
                             base_col = col
                             
                             for suffix in potential_suffixes:
                                 # Try replacing last part
                                 parts = base_col.split('|')
                                 if parts[-1] in ['negotiated_dollar', 'estimated_amount']:
                                     parts[-1] = suffix
                                     sibling_col = "|".join(parts)
                                     
                                     if sibling_col in row and not pd.isna(row[sibling_col]):
                                         sibling_val = str(row[sibling_col]).strip()
                                         if sibling_val and sibling_val != '':
                                             price_note = f"{suffix}: {sibling_val}"
                                             break
                        
                        if price_val is not None or price_note is not None:
                            # Parse Payer/Plan from column header
                            # Example: standard_charge|United Healthcare|UnitedHealthcareNewBusiness|negotiated_dollar
                            parts = col.split('|')
                            
                            if len(parts) >= 3:
                                # Assuming standard_charge|PAYER|PLAN|...
                                # Sometimes it might be: estimated_amount|PAYER|PLAN
                                
                                # Find index of Payer. Usually index 1.
                                payer_name = parts[1]
                                plan_name = parts[2] if len(parts) > 2 else None
                                
                                # Clean up if last part is 'negotiated_dollar' etc.
                                if plan_name in ['negotiated_dollar', 'estimated_amount', 'negotiated_percentage']:
                                    plan_name = None
                                    
                                session.add(Price(item_id=item_id, payer=payer_name, plan=plan_name, amount=price_val, notes=price_note))

            # B. Gross / Cash Prices (Common to both usually)
            # Capture location/notes from the last column to distinguish duplicates
            notes_col = row.get('additional_generic_notes')
            location_info = None
            if not pd.isna(notes_col):
                location_info = str(notes_col).replace('Gross Charge Type:', '').strip()

            gross_str = row.get('standard_charge|gross')
            gross_val, gross_note = parse_price(gross_str)
            if gross_val is not None or gross_note is not None:
                 # Append location info to notes if present
                 final_note = gross_note
                 if location_info and gross_note: final_note = f"{gross_note} | {location_info}"
                 elif location_info: final_note = location_info
                 
                 session.add(Price(item_id=item_id, payer="GROSS", plan=None, amount=gross_val, notes=final_note))

            cash_str = row.get('standard_charge|discounted_cash')
            cash_val, cash_note = parse_price(cash_str)
            if cash_val is not None or cash_note is not None:
                 final_note = cash_note
                 if location_info and cash_note: final_note = f"{cash_note} | {location_info}"
                 elif location_info: final_note = location_info

                 session.add(Price(item_id=item_id, payer="DISCOUNTED_CASH", plan=None, amount=cash_val, notes=final_note))
            
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
    if len(sys.argv) < 3:
        # Fallback for testing
        print("Usage: python3 scripts/ingest_tall.py <CSV_PATH> <HOSPITAL_ID>")
    else:
        ingest_tall_csv(sys.argv[1], sys.argv[2])
