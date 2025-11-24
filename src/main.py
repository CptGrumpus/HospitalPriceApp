from fastapi import FastAPI, HTTPException, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session, joinedload
from src.database import SessionLocal, Item, Price

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
    Returns items with their associated prices.
    """
    if not q:
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")
    
    # Search logic: ILIKE for case-insensitive match
    search_term = f"%{q}%"
    
    # Join Items and Prices and filter
    items = db.query(Item).options(joinedload(Item.prices)).filter(
        (Item.description.ilike(search_term)) | (Item.code.ilike(search_term))
    ).limit(10000).all()
    
    results = []
    for item in items:
        # Filter out None prices if any
        valid_prices = [p for p in item.prices if p.amount is not None]
        
        results.append({
            "hospital_id": item.hospital_id,
            "code": item.code,
            "code_type": item.code_type,
            "description": item.description,
            "setting": item.setting,
            "prices": [
                {
                    "payer": p.payer,
                    "plan": p.plan,  # New field
                    "amount": p.amount
                }
                for p in valid_prices
            ]
        })
        
    return {"count": len(results), "results": results}
