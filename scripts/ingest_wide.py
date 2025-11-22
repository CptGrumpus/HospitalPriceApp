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

def ingest_uofm_csv(file_path):
    print(f"--- Starting Ingestion for: {file_path} ---")
    
    # Initialize DB tables
    init_db()
    
    # Create a DB session
    session = SessionLocal()

    try:
        # Read the CSV
        # Note: UofM CSV has a weird header structure. 
        # Line 1: Hospital info
        # Line 2: More info
        # Line 3: The actual headers
        # We skip rows 0 and 1.
        print("Reading CSV... (this may take a moment)")
        df = pd.read_csv(file_path, header=2, dtype=str)
        
        print(f"Loaded {len(df)} rows. Processing...")

        # Iterate through rows
        # Optimize: For speed, we can use bulk_save_objects later, but let's do simple first.
        
        count = 0
        for index, row in df.iterrows():
            # 1. Create the Item
            code = row.get('code', 'UNKNOWN')
            desc = row.get('description', 'No Description')
            
            item = Item(code=code, description=desc, hospital_id="UOFM")
            session.add(item)
            session.flush() # Get the ID of the new item

            # 2. Extract Prices (The "Melting" Phase)
            # UofM columns look like: "standard_charge|gross", "standard_charge|Aetna..."
            
            for col in df.columns:
                if 'standard_charge' in col:
                    price_val = parse_price(row[col])
                    
                    if price_val is not None:
                        # Parse the column name to get Payer
                        # Example: "standard_charge|Blue Cross...|negotiated_dollar"
                        parts = col.split('|')
                        
                        payer_name = "Unknown"
                        if len(parts) > 1:
                            if parts[1] in ['gross', 'discounted_cash', 'min', 'max']:
                                payer_name = parts[1].upper() # GROSS, CASH, MIN, MAX
                            else:
                                payer_name = parts[1] # The Insurance Company Name

                        # Create Price entry
                        price_entry = Price(
                            item_id=item.id,
                            payer=payer_name,
                            amount=price_val
                        )
                        session.add(price_entry)

            count += 1
            if count % 100 == 0:
                print(f"Processed {count} rows...")
                session.commit() # Commit every 100 rows to be safe
                
            # DEBUG LIMIT: Stop after 500 rows for testing
            if count >= 500:
                print("--- DEBUG LIMIT REACHED (500 rows) ---")
                break

        session.commit()
        print("--- Ingestion Complete ---")

    except Exception as e:
        print(f"Error: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    # Default path to the downloaded file
    csv_path = "data/raw/38-6006309_UNIVERSITY-OF-MICHIGAN-HEALTH_standardcharges.csv"
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
    else:
        ingest_uofm_csv(csv_path)

