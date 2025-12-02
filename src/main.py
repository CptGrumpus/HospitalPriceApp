from fastapi import FastAPI, HTTPException, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session, joinedload
from src.database import SessionLocal, Item, Price, CodeDefinition
import statistics

app = FastAPI(title="Hospital Price API")

# Serve static files (CSS, JS, HTML)
app.mount("/static", StaticFiles(directory="src/static"), name="static")

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
def read_root():
    return FileResponse('src/static/index.html')

@app.get("/search")
def search_items(q: str, db: Session = Depends(get_db)):
    """
    Search for items by description or code.
    Returns merged items with aggregated prices and statistics.
    """
    if not q:
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")
    
    # Search logic: ILIKE for case-insensitive match
    search_term = f"%{q}%"
    
    # Join Items and Prices and filter
    items = db.query(Item).options(joinedload(Item.prices)).filter(
        (Item.description.ilike(search_term)) | (Item.code.ilike(search_term))
    ).limit(10000).all()
    
    # Fetch definitions for all codes found
    found_codes = [item.code for item in items if item.code]
    definitions = {}
    if found_codes:
        defs = db.query(CodeDefinition).filter(CodeDefinition.code.in_(found_codes)).all()
        definitions = {d.code: d for d in defs}

    # GROUPING LOGIC: Merge duplicates (Same Hospital + Same Code)
    merged_map = {} 
    seen_prices = set()

    for item in items:
        group_key = (item.hospital_id, item.code)
        
        if group_key not in merged_map:
            def_obj = definitions.get(item.code, None)
            
            ai_title = None
            ai_desc = None
            official_desc = None
            
            if def_obj:
                official_desc = def_obj.long_description
                if def_obj.generated_title and def_obj.generated_title != "Unknown Procedure":
                    ai_title = def_obj.generated_title
                    ai_desc = def_obj.generated_description

            merged_map[group_key] = {
                "hospital_id": item.hospital_id,
                "code": item.code,
                "code_type": item.code_type,
                "description": item.description,
                "ai_title": ai_title,
                "ai_description": ai_desc,
                "official_definition": official_desc,
                "setting": item.setting,
                "prices": [],
                "stats": None
            }
        
        for p in item.prices:
            if p.amount is None and (not p.notes or len(p.notes) == 0):
                continue

            price_key = (item.hospital_id, item.code, p.payer, p.plan, p.amount)
            if price_key in seen_prices:
                continue 
            
            seen_prices.add(price_key)
            
            # Context Logic: ALWAYS append specific item description if it adds context
            final_notes = p.notes or ""
            
            # Only append if item description is useful and distinct
            # For the J1815 case, item.description is "INSULIN... CONCENTRATE"
            # We want that in the notes.
            if item.description:
                 if final_notes:
                     # Avoid duplicating if note already contains description
                     if item.description not in final_notes:
                        final_notes += f" | {item.description}"
                 else:
                     final_notes = item.description

            merged_map[group_key]["prices"].append({
                "payer": p.payer,
                "plan": p.plan,
                "amount": p.amount,
                "notes": final_notes
            })

    # Calculate Stats
    results = []
    for merged_item in merged_map.values():
        prices_list = [p['amount'] for p in merged_item['prices'] if p['amount'] is not None]
        
        if prices_list:
            merged_item['stats'] = {
                "min": min(prices_list),
                "max": max(prices_list),
                "median": statistics.median(prices_list),
                "count": len(prices_list)
            }
        else:
            merged_item['stats'] = {
                "min": 0, "max": 0, "median": 0, "count": 0
            }
            
        results.append(merged_item)
        
    return {"count": len(results), "results": results}
