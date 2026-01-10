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

# Import shared extraction functions
sys.path.insert(0, str(Path(__file__).parent))
from extractors import (
    safe_get_value,
    parse_json_value,
    extract_code_from_value,
    extract_code,
    extract_setting,
    PriceExtractor
)

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


# extract_code and extract_setting are now imported from extractors.py
# Note: The shared extractors.py functions handle all the logic needed


def sanitize_filename(name):
    """Create a safe directory/file name (matches download_all.py)."""
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
    return safe[:100]  # Limit length


def find_data_file(hospital_name):
    """Find the data file for a hospital."""
    # Use the same folder naming as download_all.py
    safe_name = sanitize_filename(hospital_name)
    hospital_dir = DOWNLOADS_DIR / safe_name
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


# Header row fallback logic removed - Phase 2 should detect header_row correctly


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
        setting = extract_setting(row, config, is_json=False)
        
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
        
        # Auto-detect header style: if payer names are in column names, treat as header style
        if payer_style == 'column':
            # Check if we have columns with payer names embedded (header style)
            header_style_cols = [col for col in df.columns if 
                                ('negotiated_dollar' in str(col) or 'estimated_amount' in str(col)) and
                                '|' in str(col) and len(str(col).split('|')) >= 2]
            if header_style_cols:
                # Found columns with payer names in them - this is actually header style
                payer_style = 'header'
        
        # Extract prices based on payer style
        if payer_style == 'column':
            # Payer/plan are in columns
            payer_col = price_ext.get('payer_column', 'payer_name')
            plan_col = price_ext.get('plan_column', 'plan_name')
            price_col = price_ext.get('price_column', 'standard_charge|negotiated_dollar')
            percentage_col = price_ext.get('percentage_column', 'standard_charge|negotiated_percentage')
            methodology_col = price_ext.get('methodology_column', 'standard_charge|methodology')
            
            payer = row.get(payer_col) if payer_col else None
            plan = row.get(plan_col) if plan_col else None
            
            if payer and not pd.isna(payer):
                payer = str(payer).strip()
                plan = str(plan).strip() if plan and not pd.isna(plan) else None
                
                price_val, price_note = parse_price(row.get(price_col), skip_rules)
                
                # If no dollar amount, check for percentage-based pricing
                if price_val is None and price_note is None:
                    percentage_val = None
                    methodology_val = None
                    
                    # Check for percentage column
                    if percentage_col and percentage_col in row and not pd.isna(row.get(percentage_col)):
                        pct_str = str(row.get(percentage_col)).strip()
                        if pct_str and pct_str != 'nan':
                            try:
                                percentage_val = float(pct_str)
                            except:
                                percentage_val = pct_str  # Keep as string if not numeric
                    
                    # Check for methodology column
                    if methodology_col and methodology_col in row and not pd.isna(row.get(methodology_col)):
                        meth_str = str(row.get(methodology_col)).strip()
                        if meth_str and meth_str != 'nan':
                            methodology_val = meth_str
                    
                    # If we have percentage, create a note
                    if percentage_val is not None:
                        if isinstance(percentage_val, float):
                            price_note = f"PERCENTAGE: {percentage_val}%"
                        else:
                            price_note = f"PERCENTAGE: {percentage_val}"
                        
                        if methodology_val:
                            price_note += f" ({methodology_val})"
                
                # Check sibling columns if still no price/note
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
        setting = extract_setting(row, config, is_json=False)
        
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


def ingest_json(data, config, hospital_id, session, skip_rules):
    """
    Ingest a JSON file where data is a list of records or dict with nested arrays.
    """
    price_ext = config.get('price_extraction', {})
    desc_col = config.get('description_column', 'description')
    notes_col = config.get('notes_column')
    
    # Item cache to avoid duplicates
    item_cache = {}  # (code, description, setting) -> item_id
    price_dedupe = set()  # (item_id, payer, plan, amount, notes)
    
    items_created = 0
    prices_created = 0
    
    # Handle different JSON structures
    records = None
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        # Look for common array keys
        for key in ['standard_charge_information', 'data', 'items', 'records']:
            if key in data and isinstance(data[key], list):
                records = data[key]
                break
    
    if not records:
        raise ValueError("Could not parse JSON structure - no records found")
    
    print(f"  Processing {len(records):,} JSON records...")
    
    for idx, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        
        # Extract code (handles nested JSON structures)
        code, code_type = extract_code(record, config, is_json=True)
        if not code or code == 'UNKNOWN':
            continue
        
        # Extract description
        desc = record.get(desc_col, 'No Description')
        if desc is None or (isinstance(desc, str) and desc.strip() == ''):
            desc = 'No Description'
        desc = str(desc).strip()
        
        # Extract setting (handles nested JSON structures)
        setting = extract_setting(record, config, is_json=True)
        
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
        
        # Extract prices from nested standard_charges structure
        if 'standard_charges' in record:
            sc_val = record.get('standard_charges')
            sc_parsed = parse_json_value(sc_val)
            
            if isinstance(sc_parsed, list) and len(sc_parsed) > 0:
                # standard_charges is a list of charge objects
                for charge_obj in sc_parsed:
                    if not isinstance(charge_obj, dict):
                        continue
                    
                    # Extract gross charge
                    if 'gross_charge' in charge_obj:
                        gross_val = charge_obj['gross_charge']
                        if gross_val is not None:
                            price_val, price_note = parse_price(gross_val, skip_rules)
                            if price_val is not None or price_note is not None:
                                dedupe_key = (item_id, 'GROSS', None, price_val, price_note)
                                if dedupe_key not in price_dedupe:
                                    session.add(Price(item_id=item_id, payer='GROSS', plan=None, amount=price_val, notes=price_note))
                                    price_dedupe.add(dedupe_key)
                                    prices_created += 1
                    
                    # Extract discounted cash
                    if 'discounted_cash' in charge_obj:
                        cash_val = charge_obj['discounted_cash']
                        if cash_val is not None:
                            price_val, price_note = parse_price(cash_val, skip_rules)
                            if price_val is not None or price_note is not None:
                                dedupe_key = (item_id, 'DISCOUNTED_CASH', None, price_val, price_note)
                                if dedupe_key not in price_dedupe:
                                    session.add(Price(item_id=item_id, payer='DISCOUNTED_CASH', plan=None, amount=price_val, notes=price_note))
                                    price_dedupe.add(dedupe_key)
                                    prices_created += 1
                    
                    # Extract from payers_information array (insurance prices)
                    if 'payers_information' in charge_obj:
                        payers_info = charge_obj['payers_information']
                        if isinstance(payers_info, list):
                            for payer_obj in payers_info:
                                if isinstance(payer_obj, dict):
                                    payer_name = payer_obj.get('payer_name') or payer_obj.get('payer')
                                    plan_name = payer_obj.get('plan_name') or payer_obj.get('plan')
                                    estimated = payer_obj.get('estimated_amount') or payer_obj.get('negotiated_dollar')
                                    
                                    if payer_name:
                                        payer_name = str(payer_name).strip()
                                        plan_name = str(plan_name).strip() if plan_name else None
                                        
                                        # Try to extract dollar amount first
                                        price_val, price_note = parse_price(estimated, skip_rules) if estimated is not None else (None, None)
                                        
                                        # If no dollar amount, check for percentage-based pricing
                                        if price_val is None and price_note is None:
                                            percentage = payer_obj.get('negotiated_percentage') or payer_obj.get('percentage')
                                            methodology = payer_obj.get('methodology') or payer_obj.get('methodology_type')
                                            
                                            if percentage is not None:
                                                pct_str = str(percentage).strip()
                                                try:
                                                    pct_float = float(pct_str)
                                                    price_note = f"PERCENTAGE: {pct_float}%"
                                                except:
                                                    price_note = f"PERCENTAGE: {pct_str}"
                                                
                                                if methodology:
                                                    price_note += f" ({str(methodology).strip()})"
                                        
                                        # Add notes from payer object if available
                                        additional_notes = payer_obj.get('additional_payer_notes')
                                        if additional_notes and price_note:
                                            price_note = f"{price_note}; {str(additional_notes).strip()}"
                                        elif additional_notes:
                                            price_note = str(additional_notes).strip()
                                        
                                        if price_val is not None or price_note is not None:
                                            dedupe_key = (item_id, payer_name, plan_name, price_val, price_note)
                                            if dedupe_key not in price_dedupe:
                                                session.add(Price(item_id=item_id, payer=payer_name, plan=plan_name, amount=price_val, notes=price_note))
                                                price_dedupe.add(dedupe_key)
                                                prices_created += 1
                    
                    # Extract negotiated prices (payer-specific) - fallback for other structures
                    payer_style = price_ext.get('payer_style', 'column')
                    if payer_style == 'column':
                        # Payer/plan are in separate fields
                        payer_col = price_ext.get('payer_column', 'payer_name')
                        plan_col = price_ext.get('plan_column', 'plan_name')
                        price_col = price_ext.get('price_column', 'negotiated_dollar')
                        
                        # Check if payer info is in charge_obj or parent record
                        payer = charge_obj.get(payer_col) or record.get(payer_col)
                        plan = charge_obj.get(plan_col) or record.get(plan_col)
                        price_val_raw = charge_obj.get(price_col) or record.get(price_col)
                        
                        if payer and price_val_raw is not None:
                            payer = str(payer).strip()
                            plan = str(plan).strip() if plan else None
                            price_val, price_note = parse_price(price_val_raw, skip_rules)
                            
                            if price_val is not None or price_note is not None:
                                dedupe_key = (item_id, payer, plan, price_val, price_note)
                                if dedupe_key not in price_dedupe:
                                    session.add(Price(item_id=item_id, payer=payer, plan=plan, amount=price_val, notes=price_note))
                                    price_dedupe.add(dedupe_key)
                                    prices_created += 1
                    else:
                        # Payer style = 'header' - scan for negotiated prices in charge_obj
                        for key, val in charge_obj.items():
                            if ('negotiated' in key.lower() or 'estimated' in key.lower()) and isinstance(val, (int, float)):
                                # Try to parse payer from key name
                                payer = key.replace('_', ' ').title()[:50]
                                price_val, price_note = parse_price(val, skip_rules)
                                if price_val is not None or price_note is not None:
                                    dedupe_key = (item_id, payer, None, price_val, price_note)
                                    if dedupe_key not in price_dedupe:
                                        session.add(Price(item_id=item_id, payer=payer, plan=None, amount=price_val, notes=price_note))
                                        price_dedupe.add(dedupe_key)
                                        prices_created += 1
            elif isinstance(sc_parsed, dict):
                # standard_charges is a single object
                charge_obj = sc_parsed
                
                # Extract gross charge
                if 'gross_charge' in charge_obj:
                    gross_val = charge_obj['gross_charge']
                    if gross_val is not None:
                        price_val, price_note = parse_price(gross_val, skip_rules)
                        if price_val is not None or price_note is not None:
                            dedupe_key = (item_id, 'GROSS', None, price_val, price_note)
                            if dedupe_key not in price_dedupe:
                                session.add(Price(item_id=item_id, payer='GROSS', plan=None, amount=price_val, notes=price_note))
                                price_dedupe.add(dedupe_key)
                                prices_created += 1
                
                # Extract discounted cash
                if 'discounted_cash' in charge_obj:
                    cash_val = charge_obj['discounted_cash']
                    if cash_val is not None:
                        price_val, price_note = parse_price(cash_val, skip_rules)
                        if price_val is not None or price_note is not None:
                            dedupe_key = (item_id, 'DISCOUNTED_CASH', None, price_val, price_note)
                            if dedupe_key not in price_dedupe:
                                session.add(Price(item_id=item_id, payer='DISCOUNTED_CASH', plan=None, amount=price_val, notes=price_note))
                                price_dedupe.add(dedupe_key)
                                prices_created += 1
                
                # Extract from payers_information array (insurance prices)
                if 'payers_information' in charge_obj:
                    payers_info = charge_obj['payers_information']
                    if isinstance(payers_info, list):
                        for payer_obj in payers_info:
                            if isinstance(payer_obj, dict):
                                payer_name = payer_obj.get('payer_name') or payer_obj.get('payer')
                                plan_name = payer_obj.get('plan_name') or payer_obj.get('plan')
                                estimated = payer_obj.get('estimated_amount') or payer_obj.get('negotiated_dollar')
                                
                                if payer_name:
                                    payer_name = str(payer_name).strip()
                                    plan_name = str(plan_name).strip() if plan_name else None
                                    
                                    # Try to extract dollar amount first
                                    price_val, price_note = parse_price(estimated, skip_rules) if estimated is not None else (None, None)
                                    
                                    # If no dollar amount, check for percentage-based pricing
                                    if price_val is None and price_note is None:
                                        percentage = payer_obj.get('negotiated_percentage') or payer_obj.get('percentage')
                                        methodology = payer_obj.get('methodology') or payer_obj.get('methodology_type')
                                        
                                        if percentage is not None:
                                            pct_str = str(percentage).strip()
                                            try:
                                                pct_float = float(pct_str)
                                                price_note = f"PERCENTAGE: {pct_float}%"
                                            except:
                                                price_note = f"PERCENTAGE: {pct_str}"
                                            
                                            if methodology:
                                                price_note += f" ({str(methodology).strip()})"
                                    
                                    # Add notes from payer object if available
                                    additional_notes = payer_obj.get('additional_payer_notes')
                                    if additional_notes and price_note:
                                        price_note = f"{price_note}; {str(additional_notes).strip()}"
                                    elif additional_notes:
                                        price_note = str(additional_notes).strip()
                                    
                                    if price_val is not None or price_note is not None:
                                        dedupe_key = (item_id, payer_name, plan_name, price_val, price_note)
                                        if dedupe_key not in price_dedupe:
                                            session.add(Price(item_id=item_id, payer=payer_name, plan=plan_name, amount=price_val, notes=price_note))
                                            price_dedupe.add(dedupe_key)
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
    data_file = find_data_file(hospital_name)
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
            # Build list of expected columns for validation
            # Load CSV with header row from config (Phase 2 should have detected it correctly)
            try:
                df = pd.read_csv(data_file, header=header_row, dtype=str, encoding=encoding)
            except UnicodeDecodeError:
                df = pd.read_csv(data_file, header=actual_header_row, dtype=str, encoding='iso-8859-1')
            
            print(f"  Loaded {len(df):,} rows")
            
            # Ingest based on format
            if format_type == 'wide':
                items, prices = ingest_csv_wide(df, config, db_hospital_id, session, skip_rules)
            else:  # tall or unknown
                items, prices = ingest_csv_tall(df, config, db_hospital_id, session, skip_rules)
            
            return True, items, prices, None
            
        elif data_file.suffix.lower() == '.json':
            # Load JSON file
            with open(data_file, 'r', encoding=encoding) as f:
                data = json.load(f)
            
            print(f"  Loaded JSON file")
            
            # Ingest JSON
            items, prices = ingest_json(data, config, db_hospital_id, session, skip_rules)
            
            return True, items, prices, None
        
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

