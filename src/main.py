from fastapi import FastAPI, HTTPException, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session, joinedload
from src.database import SessionLocal, Item, Price, CodeDefinition

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
    Returns merged items (grouped by hospital + code) with aggregated prices.
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
        # Map code -> full definition object
        definitions = {d.code: d for d in defs}

    # GROUPING LOGIC: Merge duplicates (Same Hospital + Same Code)
    merged_map = {} # Key: (hospital_id, code) -> Item Dict

    for item in items:
        # Filter out None prices
        valid_prices = [
            {
                "payer": p.payer,
                "plan": p.plan,
                "amount": p.amount,
                "notes": p.notes 
            }
            for p in item.prices 
            if p.amount is not None or (p.notes and len(p.notes) > 0)
        ]
        
        # Key for grouping
        key = (item.hospital_id, item.code)
        
        if key not in merged_map:
            # Create new entry
            def_obj = definitions.get(item.code, None)
            
            ai_title = None
            ai_desc = None
            official_desc = None
            
            if def_obj:
                official_desc = def_obj.long_description
                if def_obj.generated_title and def_obj.generated_title != "Unknown Procedure":
                    ai_title = def_obj.generated_title
                    ai_desc = def_obj.generated_description

            merged_map[key] = {
                "hospital_id": item.hospital_id,
                "code": item.code,
                "code_type": item.code_type,
                "description": item.description, # Keep first description found
                "ai_title": ai_title,
                "ai_description": ai_desc,
                "official_definition": official_desc,
                "setting": item.setting,
                "prices": []
            }
        
        # Aggregate prices
        merged_map[key]["prices"].extend(valid_prices)

    # Convert back to list
    results = list(merged_map.values())
        
    return {"count": len(results), "results": results}
