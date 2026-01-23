from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
from src.database import SessionLocal, Item, Price, CodeDefinition, engine
import statistics
import time

app = FastAPI(title="Hospital Price API")

# Serve static files (CSS, JS, HTML)
app.mount("/static", StaticFiles(directory="src/static"), name="static")

# Check if FTS5 table exists at startup
_fts_available = None

def check_fts_available():
    """Check if FTS5 index is available."""
    global _fts_available
    if _fts_available is None:
        try:
            with engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='items_fts'"
                ))
                _fts_available = result.fetchone() is not None
        except:
            _fts_available = False
        print(f"FTS5 search: {'enabled' if _fts_available else 'disabled (run scripts/create_fts_index.py)'}")
    return _fts_available

# Check on startup
@app.on_event("startup")
async def startup_event():
    check_fts_available()

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
def search_items(
    q: str,
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=200, description="Results per page"),
    db: Session = Depends(get_db)
):
    """
    Search for items by description or code.
    Returns paginated results with aggregated prices and statistics.
    
    Uses FTS5 full-text search if available (100-1000x faster).
    """
    if not q:
        raise HTTPException(status_code=400, detail="Query parameter 'q' is required")
    
    start_time = time.time()
    offset = (page - 1) * limit
    
    # Determine search strategy
    use_fts = check_fts_available() and len(q) >= 2
    
    if use_fts:
        # FTS5 search - dramatically faster for text queries
        # Escape special FTS5 characters and create search term
        search_term = q.replace('"', '""').replace("'", "''")
        
        # For partial matches, use prefix search with *
        # "MRI" becomes "MRI*" to match "MRI", "MRIS", etc.
        fts_query = f'"{search_term}"*'
        
        # First, get matching item IDs from FTS (very fast)
        fts_sql = text("""
            SELECT rowid FROM items_fts 
            WHERE items_fts MATCH :query
            LIMIT :limit OFFSET :offset
        """)
        
        # Get total count for pagination (also fast with FTS)
        count_sql = text("""
            SELECT COUNT(*) FROM items_fts WHERE items_fts MATCH :query
        """)
        
        try:
            # Get count
            count_result = db.execute(count_sql, {"query": fts_query})
            total_count = count_result.scalar() or 0
            
            # Get matching IDs
            result = db.execute(fts_sql, {"query": fts_query, "limit": limit, "offset": offset})
            item_ids = [row[0] for row in result]
            
            if not item_ids:
                return {
                    "count": 0,
                    "total": total_count,
                    "page": page,
                    "limit": limit,
                    "pages": 0,
                    "search_time_ms": int((time.time() - start_time) * 1000),
                    "results": []
                }
            
            # Load actual items by ID (fast with index)
            items = db.query(Item).filter(Item.id.in_(item_ids)).all()
            
        except Exception as e:
            # Fall back to regular search if FTS fails
            print(f"FTS search failed, falling back to LIKE: {e}")
            use_fts = False
    
    if not use_fts:
        # Fallback: Regular LIKE search (slower but always works)
        search_term = f"%{q}%"
        
        # Get total count
        total_count = db.query(Item).filter(
            (Item.description.ilike(search_term)) | (Item.code.ilike(search_term))
        ).count()
        
        # Get paginated items (NO eager loading of prices)
        items = db.query(Item).filter(
            (Item.description.ilike(search_term)) | (Item.code.ilike(search_term))
        ).offset(offset).limit(limit).all()
    
    # Now load prices only for the items we're returning (much smaller set)
    item_ids = [item.id for item in items]
    prices_by_item = {}
    
    if item_ids:
        prices = db.query(Price).filter(Price.item_id.in_(item_ids)).all()
        for price in prices:
            if price.item_id not in prices_by_item:
                prices_by_item[price.item_id] = []
            prices_by_item[price.item_id].append(price)
    
    # Fetch definitions for all codes found
    found_codes = list(set(item.code for item in items if item.code))
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
        
        # Get prices for this item
        item_prices = prices_by_item.get(item.id, [])
        for p in item_prices:
            if p.amount is None and (not p.notes or len(p.notes) == 0):
                continue

            price_key = (item.hospital_id, item.code, p.payer, p.plan, p.amount)
            if price_key in seen_prices:
                continue 
            
            seen_prices.add(price_key)
            
            # Context Logic: ALWAYS append specific item description if it adds context
            final_notes = p.notes or ""
            
            if item.description:
                if final_notes:
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
    
    # Calculate pagination info
    total_pages = (total_count + limit - 1) // limit if total_count > 0 else 0
    search_time = int((time.time() - start_time) * 1000)
    
    return {
        "count": len(results),
        "total": total_count,
        "page": page,
        "limit": limit,
        "pages": total_pages,
        "search_time_ms": search_time,
        "fts_enabled": use_fts,
        "results": results
    }


@app.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    """Get database statistics."""
    item_count = db.query(Item).count()
    price_count = db.query(Price).count()
    hospital_count = db.execute(text("SELECT COUNT(DISTINCT hospital_id) FROM items")).scalar()
    
    return {
        "items": item_count,
        "prices": price_count,
        "hospitals": hospital_count,
        "fts_enabled": check_fts_available()
    }
