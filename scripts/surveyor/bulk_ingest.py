#!/usr/bin/env python3
"""
Phase 5: Universal Bulk Ingestor

Ingests all validated hospitals into the database using AI-generated configs.
Replaces ingest_tall.py and ingest_wide.py with a single universal script.

Features:
- Only ingests validated hospitals (unless --force-all)
- Re-ingest single hospital with --hospital-id
- Uses enhanced config schema (code_extraction, price_extraction)
- Idempotent: deletes existing data before re-ingesting
- Tracks ingestion status in manifest
"""

import json
import os
import sys
import re
from pathlib import Path
from datetime import datetime
import argparse

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text

from src.database import SessionLocal, Item, Price, init_db

# Configuration
DATA_DIR = Path(__file__).parent.parent.parent / "data"
CONFIGS_DIR = DATA_DIR / "configs"
DOWNLOADS_DIR = DATA_DIR / "downloads"
CONFIG_MANIFEST = CONFIGS_DIR / "config_manifest.json"

# Price parsing settings
PLACEHOLDER_THRESHOLD = 99999999
FORMULA_PATTERNS = ['formula', 'algorithm', 'see contract', 'varies', 'call for pricing']


def load_config_manifest():
    """Load the config manifest."""
    if not CONFIG_MANIFEST.exists():
        print(f"ERROR: Config manifest not found at {CONFIG_MANIFEST}")
        sys.exit(1)
    with open(CONFIG_MANIFEST, 'r') as f:
        return json.load(f)


def save_config_manifest(manifest):
    """Save config manifest."""
    manifest["last_updated"] = datetime.now().isoformat()
    with open(CONFIG_MANIFEST, 'w') as f:
        json.dump(manifest, f, indent=2)


def load_config(hospital_id):
    """Load a hospital's config file."""
    config_file = CONFIGS_DIR / f"{hospital_id}.json"
    if not config_file.exists():
        return None
    with open(config_file, 'r') as f:
        return json.load(f)


def slugify(name):
    """Convert hospital name to a clean database ID."""
    # Remove special characters, replace spaces with underscores
    slug = re.sub(r'[^\w\s-]', '', name)
    slug = re.sub(r'[-\s]+', '_', slug)
    return slug.upper()[:50]


def parse_price(price_str, skip_rules=None):
    """
    Parse a price string into (value, notes).
    Returns (float_value, notes_string) or (None, notes_string) if unparseable.
    """
    if pd.isna(price_str) or price_str == '' or str(price_str).strip() == '':
        return None, None
    
    price_str_clean = str(price_str).strip()
    
    # Check for formula/algorithm patterns
    skip_rules = skip_rules or {}
    formula_patterns = skip_rules.get('formula_patterns', FORMULA_PATTERNS)
    
    for pattern in formula_patterns:
        if pattern.lower() in price_str_clean.lower():
            return None, price_str_clean
    
    try:
        # Remove $ and , and convert to float
        clean = price_str_clean.replace('$', '').replace(',', '')
        val = float(clean)
        
        # Filter placeholder values
        threshold = skip_rules.get('placeholder_threshold', PLACEHOLDER_THRESHOLD)
        if val >= threshold:
            return None, None
        
        return val, None
    except:
        # Return as note if can't parse
        return None, price_str_clean if price_str_clean else None


def extract_code(row, config):
    """
    Extract the best code from a row using the config's code_extraction rules.
    Returns (code, code_type).
    """
    code_ext = config.get('code_extraction', {})
    columns = code_ext.get('columns', [])
    type_columns = code_ext.get('type_columns', [])
    priority = code_ext.get('priority', ['CPT', 'HCPCS', 'MS-DRG', 'APR-DRG', 'NDC', 'CDM', 'Local'])
    auto_normalize = code_ext.get('auto_normalize', True)
    
    # Build priority map (lower = better)
    priority_map = {code_type: i for i, code_type in enumerate(priority)}
    priority_map['UNKNOWN'] = 999
    
    # Fallback: if no code_extraction, use old-style single column
    if not columns:
        code_col = config.get('code_column', 'code|1')
        type_col = config.get('code_type_column')
        code = row.get(code_col, 'UNKNOWN')
        code_type = row.get(type_col, 'UNKNOWN') if type_col else 'UNKNOWN'
        return str(code).strip() if code else 'UNKNOWN', code_type
    
    # Find best code by priority
    best_code = 'UNKNOWN'
    best_type = 'UNKNOWN'
    best_priority = 999
    
    for i, col in enumerate(columns):
        if col not in row:
            continue
        
        code = row.get(col)
        if pd.isna(code) or str(code).strip() == '':
            continue
        
        code = str(code).strip()
        
        # Get type from corresponding type column
        code_type = 'UNKNOWN'
        if type_columns and i < len(type_columns):
            type_col = type_columns[i]
            if type_col and type_col in row:
                code_type = str(row.get(type_col, 'UNKNOWN')).strip()
        
        # Validate CPT/HCPCS codes (must be 5 chars)
        if code_type in ['CPT', 'HCPCS']:
            if len(code) != 5:
                code_type = 'Local'  # Downgrade invalid codes
        
        # Check priority
        this_priority = priority_map.get(code_type, 999)
        if this_priority < best_priority:
            best_code = code
            best_type = code_type
            best_priority = this_priority
    
    # Auto-normalize code type based on format
    if auto_normalize and len(best_code) == 5:
        if best_code.isdigit():
            best_type = 'CPT'
        elif best_code[0].isalpha() and best_code[1:].isdigit():
            best_type = 'HCPCS'
    
    return best_code, best_type


def extract_setting(row, config):
    """Extract the setting (inpatient/outpatient) from a row."""
    setting_ext = config.get('setting_extraction', {})
    
    primary = setting_ext.get('primary', 'setting')
    fallback = setting_ext.get('fallback', 'billing_class')
    default = setting_ext.get('default', 'UNKNOWN')
    
    # Try primary column
    if primary and primary in row:
        val = row.get(primary)
        if not pd.isna(val) and str(val).strip():
            return str(val).strip()
    
    # Try fallback
    if fallback and fallback in row:
        val = row.get(fallback)
        if not pd.isna(val) and str(val).strip():
            return str(val).strip()
    
    return default


def find_data_file(hospital_id):
    """Find the data file for a hospital."""
    hospital_dir = DOWNLOADS_DIR / hospital_id
    if not hospital_dir.exists():
        return None
    
    # Check extracted folder first (for ZIPs)
    extracted_dir = hospital_dir / "extracted"
    if extracted_dir.exists():
        for f in sorted(extracted_dir.iterdir(), key=lambda x: x.stat().st_size, reverse=True):
            if f.suffix.lower() in ['.csv', '.json']:
                return f
    
    # Check main folder
    for f in sorted(hospital_dir.iterdir(), key=lambda x: x.stat().st_size, reverse=True):
        if f.suffix.lower() in ['.csv', '.json']:
            return f
    
    return None


def delete_hospital_data(session, hospital_id):
    """Delete all existing data for a hospital."""
    # Get all item IDs for this hospital
    items = session.query(Item).filter(Item.hospital_id == hospital_id).all()
    item_ids = [item.id for item in items]
    
    if item_ids:
        # Delete prices for these items
        session.query(Price).filter(Price.item_id.in_(item_ids)).delete(synchronize_session=False)
        # Delete items
        session.query(Item).filter(Item.hospital_id == hospital_id).delete(synchronize_session=False)
        session.commit()
    
    return len(item_ids)


def ingest_csv_tall(df, config, hospital_id, session, skip_rules):
    """
    Ingest a tall-format CSV where each row = one price entry.
    """
    price_ext = config.get('price_extraction', {})
    payer_style = price_ext.get('payer_style', 'column')
    desc_col = config.get('description_column', 'description')
    notes_col = config.get('notes_column')
    
    # Item cache to avoid duplicates
    item_cache = {}  # (code, description, setting) -> item_id
    price_dedupe = set()  # (item_id, payer, plan, amount, notes)
    
    items_created = 0
    prices_created = 0
    
    for idx, row in df.iterrows():
        # Extract code
        code, code_type = extract_code(row, config)
        if not code or code == 'UNKNOWN':
            continue
        
        # Extract description
        desc = row.get(desc_col, 'No Description')
        if pd.isna(desc):
            desc = 'No Description'
        desc = str(desc).strip()
        
        # Extract setting
        setting = extract_setting(row, config)
        
        # Get or create item
        item_key = (code, desc, setting)
        if item_key in item_cache:
            item_id = item_cache[item_key]
        else:
            item = Item(
                code=code,
                code_type=code_type,
                description=desc,
                hospital_id=hospital_id,
                setting=setting
            )
            session.add(item)
            session.flush()
            item_id = item.id
            item_cache[item_key] = item_id
            items_created += 1
        
        # Extract prices based on payer style
        if payer_style == 'column':
            # Payer/plan are in columns
            payer_col = price_ext.get('payer_column', 'payer_name')
            plan_col = price_ext.get('plan_column', 'plan_name')
            price_col = price_ext.get('price_column', 'standard_charge|negotiated_dollar')
            
            payer = row.get(payer_col) if payer_col else None
            plan = row.get(plan_col) if plan_col else None
            
            if payer and not pd.isna(payer):
                payer = str(payer).strip()
                plan = str(plan).strip() if plan and not pd.isna(plan) else None
                
                price_val, price_note = parse_price(row.get(price_col), skip_rules)
                
                # Check sibling columns if no price
                if price_val is None and price_note is None:
                    for sibling in price_ext.get('sibling_columns', []):
                        sibling_col = price_col.rsplit('|', 1)[0] + '|' + sibling if '|' in price_col else sibling
                        if sibling_col in row and not pd.isna(row.get(sibling_col)):
                            sibling_val = str(row.get(sibling_col)).strip()
                            if sibling_val:
                                price_note = f"{sibling}: {sibling_val}"
                                break
                
                if price_val is not None or price_note is not None:
                    dedupe_key = (item_id, payer, plan, price_val, price_note)
                    if dedupe_key not in price_dedupe:
                        session.add(Price(item_id=item_id, payer=payer, plan=plan, amount=price_val, notes=price_note))
                        price_dedupe.add(dedupe_key)
                        prices_created += 1
        
        else:
            # Payer style = 'header' - payers are in column names
            # Scan all columns for price patterns
            for col in df.columns:
                if 'negotiated_dollar' in col or 'estimated_amount' in col:
                    price_val, price_note = parse_price(row.get(col), skip_rules)
                    
                    if price_val is not None or price_note is not None:
                        # Parse payer/plan from column name
                        # Format: standard_charge|Payer|Plan|negotiated_dollar
                        parts = col.split('|')
                        payer = parts[1] if len(parts) > 1 else 'Unknown'
                        plan = parts[2] if len(parts) > 2 and parts[2] not in ['negotiated_dollar', 'estimated_amount'] else None
                        
                        dedupe_key = (item_id, payer, plan, price_val, price_note)
                        if dedupe_key not in price_dedupe:
                            session.add(Price(item_id=item_id, payer=payer, plan=plan, amount=price_val, notes=price_note))
                            price_dedupe.add(dedupe_key)
                            prices_created += 1
        
        # Also capture gross and cash prices
        gross_col = price_ext.get('gross_column', 'standard_charge|gross')
        cash_col = price_ext.get('cash_column', 'standard_charge|discounted_cash')
        
        if gross_col and gross_col in row:
            gross_val, gross_note = parse_price(row.get(gross_col), skip_rules)
            if gross_val is not None or gross_note is not None:
                dedupe_key = (item_id, 'GROSS', None, gross_val, gross_note)
                if dedupe_key not in price_dedupe:
                    session.add(Price(item_id=item_id, payer='GROSS', plan=None, amount=gross_val, notes=gross_note))
                    price_dedupe.add(dedupe_key)
                    prices_created += 1
        
        if cash_col and cash_col in row:
            cash_val, cash_note = parse_price(row.get(cash_col), skip_rules)
            if cash_val is not None or cash_note is not None:
                dedupe_key = (item_id, 'DISCOUNTED_CASH', None, cash_val, cash_note)
                if dedupe_key not in price_dedupe:
                    session.add(Price(item_id=item_id, payer='DISCOUNTED_CASH', plan=None, amount=cash_val, notes=cash_note))
                    price_dedupe.add(dedupe_key)
                    prices_created += 1
        
        # Commit periodically
        if idx % 1000 == 0:
            session.commit()
    
    session.commit()
    return items_created, prices_created


def ingest_csv_wide(df, config, hospital_id, session, skip_rules):
    """
    Ingest a wide-format CSV where each row = one item with multiple price columns.
    """
    price_ext = config.get('price_extraction', {})
    desc_col = config.get('description_column', 'description')
    
    items_created = 0
    prices_created = 0
    
    for idx, row in df.iterrows():
        # Extract code
        code, code_type = extract_code(row, config)
        if not code or code == 'UNKNOWN':
            continue
        
        # Extract description
        desc = row.get(desc_col, 'No Description')
        if pd.isna(desc):
            desc = 'No Description'
        desc = str(desc).strip()
        
        # Extract setting
        setting = extract_setting(row, config)
        
        # Create item
        item = Item(
            code=code,
            code_type=code_type,
            description=desc,
            hospital_id=hospital_id,
            setting=setting
        )
        session.add(item)
        session.flush()
        items_created += 1
        
        # Scan all columns for prices
        for col in df.columns:
            if 'standard_charge' in col.lower() or 'price' in col.lower():
                price_val, price_note = parse_price(row.get(col), skip_rules)
                
                if price_val is not None or price_note is not None:
                    # Parse payer info from column name
                    parts = col.split('|')
                    
                    payer = 'Unknown'
                    plan = None
                    
                    if len(parts) > 1:
                        if parts[1].lower() in ['gross', 'discounted_cash', 'min', 'max']:
                            payer = parts[1].upper()
                        else:
                            payer = parts[1]
                            if len(parts) > 2 and parts[2] not in ['negotiated_dollar', 'estimated_amount']:
                                plan = parts[2]
                    
                    session.add(Price(item_id=item.id, payer=payer, plan=plan, amount=price_val, notes=price_note))
                    prices_created += 1
        
        # Commit periodically
        if idx % 1000 == 0:
            session.commit()
    
    session.commit()
    return items_created, prices_created


def ingest_hospital(hospital_id, config, manifest_info, session):
    """
    Ingest a single hospital.
    Returns (success, items_created, prices_created, error_message)
    """
    hospital_name = manifest_info.get('name', 'Unknown')
    db_hospital_id = slugify(hospital_name)
    
    print(f"\n{'='*60}")
    print(f"Ingesting: {hospital_name}")
    print(f"  Database ID: {db_hospital_id}")
    
    # Find data file
    data_file = find_data_file(hospital_id)
    if not data_file:
        return False, 0, 0, "Data file not found"
    
    print(f"  File: {data_file.name}")
    
    # Get config settings
    format_type = config.get('format_type', 'tall')
    header_row = config.get('header_row', 0)
    encoding = config.get('encoding', 'utf-8')
    skip_rules = config.get('skip_rules', {})
    
    print(f"  Format: {format_type}")
    print(f"  Header row: {header_row}")
    
    # Delete existing data for this hospital
    deleted = delete_hospital_data(session, db_hospital_id)
    if deleted > 0:
        print(f"  🗑️  Deleted {deleted} existing items")
    
    try:
        if data_file.suffix.lower() == '.csv':
            # Load CSV
            try:
                df = pd.read_csv(data_file, header=header_row, dtype=str, encoding=encoding)
            except UnicodeDecodeError:
                df = pd.read_csv(data_file, header=header_row, dtype=str, encoding='iso-8859-1')
            
            print(f"  Loaded {len(df):,} rows")
            
            # Ingest based on format
            if format_type == 'wide':
                items, prices = ingest_csv_wide(df, config, db_hospital_id, session, skip_rules)
            else:  # tall or unknown
                items, prices = ingest_csv_tall(df, config, db_hospital_id, session, skip_rules)
            
            return True, items, prices, None
            
        elif data_file.suffix.lower() == '.json':
            # JSON ingestion (simplified for now)
            print(f"  ⚠️  JSON ingestion not fully implemented yet")
            return False, 0, 0, "JSON format not yet supported"
        
        else:
            return False, 0, 0, f"Unsupported file type: {data_file.suffix}"
            
    except Exception as e:
        return False, 0, 0, str(e)


def main():
    parser = argparse.ArgumentParser(description='Universal Bulk Ingestor')
    parser.add_argument('--force-all', action='store_true', help='Ingest all hospitals, even unvalidated')
    parser.add_argument('--hospital-id', type=str, help='Ingest a specific hospital by ID')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be ingested without doing it')
    args = parser.parse_args()
    
    print("=" * 60)
    print("  UNIVERSAL BULK INGESTOR - Phase 5")
    print("=" * 60)
    
    # Initialize database
    print("\nInitializing database...")
    init_db()
    session = SessionLocal()
    
    # Load manifest
    manifest = load_config_manifest()
    configs = manifest.get("configs", {})
    
    # Determine which hospitals to ingest
    to_ingest = []
    
    if args.hospital_id:
        # Single hospital mode
        if args.hospital_id in configs:
            to_ingest = [(args.hospital_id, configs[args.hospital_id])]
        else:
            print(f"ERROR: Hospital ID '{args.hospital_id}' not found in manifest")
            sys.exit(1)
    else:
        # Batch mode
        for hospital_id, info in configs.items():
            if info.get('status') != 'completed':
                continue
            
            if args.force_all:
                to_ingest.append((hospital_id, info))
            elif info.get('validated') == True:
                to_ingest.append((hospital_id, info))
    
    # Stats
    total = len(configs)
    validated = sum(1 for c in configs.values() if c.get('validated') == True)
    
    print(f"\nTotal configs: {total}")
    print(f"Validated: {validated}")
    print(f"To ingest: {len(to_ingest)}")
    
    if not to_ingest:
        if args.force_all:
            print("\n❌ No hospitals to ingest!")
        else:
            print("\n❌ No validated hospitals to ingest!")
            print("   Run preview_cards.py to approve hospitals, or use --force-all")
        sys.exit(0)
    
    if args.dry_run:
        print("\n[DRY RUN] Would ingest these hospitals:")
        for hospital_id, info in to_ingest:
            print(f"  - {info.get('name', 'Unknown')}")
        sys.exit(0)
    
    # Ingest hospitals
    print(f"\nStarting ingestion of {len(to_ingest)} hospitals...")
    print("Press Ctrl+C to stop (progress is saved)\n")
    
    stats = {
        'success': 0,
        'failed': 0,
        'total_items': 0,
        'total_prices': 0
    }
    
    try:
        for i, (hospital_id, info) in enumerate(to_ingest):
            print(f"[{i+1}/{len(to_ingest)}]", end="")
            
            config = load_config(hospital_id)
            if not config:
                print(f"  ❌ Config file not found")
                stats['failed'] += 1
                continue
            
            success, items, prices, error = ingest_hospital(hospital_id, config, info, session)
            
            if success:
                print(f"  ✅ Success: {items:,} items, {prices:,} prices")
                stats['success'] += 1
                stats['total_items'] += items
                stats['total_prices'] += prices
                
                # Update manifest
                manifest['configs'][hospital_id]['ingested'] = True
                manifest['configs'][hospital_id]['ingested_at'] = datetime.now().isoformat()
                manifest['configs'][hospital_id]['items_count'] = items
                manifest['configs'][hospital_id]['prices_count'] = prices
            else:
                print(f"  ❌ Failed: {error}")
                stats['failed'] += 1
                manifest['configs'][hospital_id]['ingested'] = False
                manifest['configs'][hospital_id]['ingest_error'] = error
            
            # Save manifest periodically
            if (i + 1) % 5 == 0:
                save_config_manifest(manifest)
                
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Progress saved.")
    
    # Final save
    save_config_manifest(manifest)
    session.close()
    
    # Summary
    print("\n" + "=" * 60)
    print("  INGESTION SUMMARY")
    print("=" * 60)
    print(f"  ✅ Successful: {stats['success']}")
    print(f"  ❌ Failed: {stats['failed']}")
    print(f"  📦 Total Items: {stats['total_items']:,}")
    print(f"  💰 Total Prices: {stats['total_prices']:,}")
    print("=" * 60)
    
    # Check for new codes needing AI descriptions
    try:
        result = session.execute(text("""
            SELECT COUNT(DISTINCT code) 
            FROM items 
            WHERE code NOT IN (SELECT code FROM code_definitions WHERE generated_title IS NOT NULL)
        """))
        new_codes = result.scalar() or 0
        
        if new_codes > 0:
            print(f"\n💡 Found {new_codes:,} new codes without AI descriptions.")
            print("   Run: python3 ai_workbench/scripts/batch_generate.py")
    except:
        pass


if __name__ == "__main__":
    main()

