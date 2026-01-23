#!/usr/bin/env python3
"""
Create FTS5 (Full-Text Search) index for the items table.
This dramatically speeds up text searches from minutes to milliseconds.

FTS5 creates an inverted index - instead of scanning 8M rows for "MRI",
it looks up "MRI" in the index and finds all matching row IDs instantly.

Run this once after bulk ingestion:
    python3 scripts/create_fts_index.py
"""

import sqlite3
import time
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "hospital.db"

def create_fts_index():
    """Create FTS5 virtual table for fast text search."""
    
    print(f"📊 Opening database: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    # Check current row count
    cursor.execute("SELECT COUNT(*) FROM items")
    item_count = cursor.fetchone()[0]
    print(f"   Found {item_count:,} items to index")
    
    # Check if FTS table already exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='items_fts'")
    if cursor.fetchone():
        print("\n⚠️  FTS table 'items_fts' already exists.")
        response = input("   Drop and recreate? (y/n): ").strip().lower()
        if response != 'y':
            print("   Skipping FTS creation.")
            conn.close()
            return
        print("   Dropping existing FTS table...")
        cursor.execute("DROP TABLE items_fts")
        conn.commit()
    
    # Create FTS5 virtual table
    # We index: code, description, code_type, hospital_id
    # content='' means we don't store the text (saves space), just the index
    # We'll join back to items table for actual data
    print("\n🔨 Creating FTS5 virtual table...")
    start = time.time()
    
    cursor.execute("""
        CREATE VIRTUAL TABLE items_fts USING fts5(
            code,
            description,
            code_type,
            hospital_id,
            content='items',
            content_rowid='id'
        )
    """)
    conn.commit()
    print(f"   Table created in {time.time() - start:.1f}s")
    
    # Populate the FTS index from existing data
    print(f"\n📝 Populating FTS index from {item_count:,} items...")
    print("   This may take several minutes for large datasets...")
    start = time.time()
    
    cursor.execute("""
        INSERT INTO items_fts(rowid, code, description, code_type, hospital_id)
        SELECT id, code, description, code_type, hospital_id FROM items
    """)
    conn.commit()
    
    elapsed = time.time() - start
    print(f"   Indexed {item_count:,} items in {elapsed:.1f}s ({item_count/elapsed:,.0f} items/sec)")
    
    # Optimize the index for faster queries
    print("\n🔧 Optimizing FTS index...")
    start = time.time()
    cursor.execute("INSERT INTO items_fts(items_fts) VALUES('optimize')")
    conn.commit()
    print(f"   Optimized in {time.time() - start:.1f}s")
    
    # Test the index with a sample query
    print("\n🧪 Testing FTS index with 'MRI' search...")
    start = time.time()
    cursor.execute("""
        SELECT COUNT(*) FROM items_fts WHERE items_fts MATCH 'MRI'
    """)
    count = cursor.fetchone()[0]
    elapsed = time.time() - start
    print(f"   Found {count:,} results in {elapsed*1000:.1f}ms")
    
    # Compare to LIKE query (don't actually run if too slow)
    print("\n📊 For comparison, LIKE '%MRI%' would scan all 8M rows...")
    
    # Check index size
    cursor.execute("SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
    # This gives total DB size, not just FTS. Let's estimate FTS size differently
    
    print("\n✅ FTS5 index created successfully!")
    print("\n💡 The search endpoint will now use FTS5 for text searches.")
    print("   Expected speedup: 100-1000x faster for text queries")
    
    conn.close()

def add_additional_indexes():
    """Add composite indexes for common query patterns."""
    
    print("\n📊 Adding additional indexes for common queries...")
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    indexes_to_create = [
        # Composite index for hospital + code lookups (for grouping)
        ("idx_items_hospital_code", "items", "(hospital_id, code)"),
        # Index on description for fallback queries
        ("idx_items_description", "items", "(description)"),
        # Index on item_id for price lookups
        ("idx_prices_item_id", "prices", "(item_id)"),
    ]
    
    for idx_name, table, columns in indexes_to_create:
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='index' AND name='{idx_name}'")
        if cursor.fetchone():
            print(f"   ⏭️  Index {idx_name} already exists")
            continue
        
        print(f"   Creating {idx_name}...")
        start = time.time()
        try:
            cursor.execute(f"CREATE INDEX {idx_name} ON {table} {columns}")
            conn.commit()
            print(f"      Done in {time.time() - start:.1f}s")
        except Exception as e:
            print(f"      Error: {e}")
    
    conn.close()
    print("✅ Additional indexes complete")

if __name__ == "__main__":
    print("=" * 60)
    print("FTS5 Full-Text Search Index Creator")
    print("=" * 60)
    
    create_fts_index()
    add_additional_indexes()
    
    print("\n" + "=" * 60)
    print("Done! Restart your FastAPI server to use the new index.")
    print("=" * 60)
