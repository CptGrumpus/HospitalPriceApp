import sys
import os

# Add the project root to the python path so we can import src
sys.path.append(os.getcwd())

from src.database import SessionLocal, Item, Price
from sqlalchemy.orm import joinedload

def check_prices():
    session = SessionLocal()
    # Codes from your screenshot/query
    codes = ['G9437', '4563F', 'G9794']
    
    print(f"Checking items for codes: {codes}")
    
    items = session.query(Item).options(joinedload(Item.prices)).filter(Item.code.in_(codes)).all()
    
    if not items:
        print("No items found with these codes.")
    
    for item in items:
        print(f"\nItem: {item.code} - {item.description}")
        print(f"Hospital: {item.hospital_id}")
        print(f"Setting: {item.setting}")
        if not item.prices:
            print("  No prices found.")
        else:
            for p in item.prices:
                price_display = f"${p.amount:,.2f}" if p.amount else "N/A"
                note_display = f" | Note: {p.notes}" if p.notes else ""
                # Only print if it's NOT the placeholder, to see if we captured ANY formulas
                if "Placeholder" not in str(p.notes):
                     print(f"  Payer: {p.payer}, Plan: {p.plan}, Amount: {price_display}{note_display}")

    session.close()

if __name__ == "__main__":
    check_prices()
