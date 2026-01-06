#!/usr/bin/env python3
"""
Phase 4: Preview Card Generator & Validation Server

Generates an HTML preview page for all hospital configs and runs a small
local server to handle approve/reject/edit actions.

Features:
- Shows config summary + sample data for each hospital
- Approve/Reject/Edit buttons update the manifest
- Filter by validation status
- Resumable - tracks validation progress
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
import html
import pandas as pd
from http.server import HTTPServer, SimpleHTTPRequestHandler
import urllib.parse
import zipfile

# Configuration
DATA_DIR = Path(__file__).parent.parent.parent / "data"
CONFIGS_DIR = DATA_DIR / "configs"
DOWNLOADS_DIR = DATA_DIR / "downloads"
PROFILES_DIR = DATA_DIR / "profiles"
CONFIG_MANIFEST = CONFIGS_DIR / "config_manifest.json"
PREVIEW_HTML = DATA_DIR / "preview_cards.html"

# Server settings
SERVER_PORT = 8765


def load_config_manifest():
    """Load the config manifest."""
    if not CONFIG_MANIFEST.exists():
        print(f"ERROR: Config manifest not found at {CONFIG_MANIFEST}")
        print("Please run generate_config.py (Phase 3) first.")
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


def load_profile(hospital_id):
    """Load a hospital's analysis profile."""
    profile_file = PROFILES_DIR / f"{hospital_id}.json"
    if not profile_file.exists():
        return None
    with open(profile_file, 'r') as f:
        return json.load(f)


def sanitize_filename(name):
    """Create a safe directory/file name (matches download_all.py)."""
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
    return safe[:100]  # Limit length


def safe_get_value(row, col_name, default=None):
    """
    Safely get a value from a pandas Series or dict, handling column name checking properly.
    """
    if isinstance(row, pd.Series):
        # For pandas Series, check index, not values
        if col_name in row.index:
            val = row[col_name]
            if pd.isna(val):
                return default
            return val
        return default
    elif isinstance(row, dict):
        # For dict, use get
        return row.get(col_name, default)
    else:
        # Fallback
        try:
            return getattr(row, col_name, default)
        except:
            return default


def parse_json_value(val):
    """
    Try to parse a value that might be JSON (string or already parsed).
    Returns the parsed value or original value.
    """
    # Handle pandas Series/array - convert to scalar first
    if isinstance(val, pd.Series):
        val = val.iloc[0] if len(val) > 0 else None
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    
    # If it's already a dict/list, return as-is
    if isinstance(val, (dict, list)):
        return val
    
    # Try to parse as JSON string
    val_str = str(val).strip()
    if not val_str or val_str == 'nan':
        return None
    
    # Check if it looks like JSON
    if val_str.startswith('[') or val_str.startswith('{'):
        try:
            return json.loads(val_str)
        except:
            pass
    
    return val


def extract_code_from_value(val):
    """
    Extract a code from a value that might be:
    - A simple string/number
    - A JSON object with 'code' key
    - A JSON array with objects containing 'code'
    """
    parsed = parse_json_value(val)
    
    if parsed is None:
        return None, None
    
    # If it's a dict, look for 'code' key
    if isinstance(parsed, dict):
        code = parsed.get('code') or parsed.get('code_value') or parsed.get('procedure_code')
        code_type = parsed.get('code_type') or parsed.get('type') or parsed.get('codeType')
        if code:
            return str(code).strip(), str(code_type).strip() if code_type else None
    
    # If it's a list, try first element
    if isinstance(parsed, list) and len(parsed) > 0:
        first = parsed[0]
        if isinstance(first, dict):
            code = first.get('code') or first.get('code_value') or first.get('procedure_code')
            code_type = first.get('code_type') or first.get('type') or first.get('codeType')
            if code:
                return str(code).strip(), str(code_type).strip() if code_type else None
    
    # If it's a simple string/number, return as-is
    if isinstance(parsed, (str, int, float)):
        return str(parsed).strip(), None
    
    return None, None


def extract_mapped_sample(row, config):
    """
    Extract mapped data from a row using the config (same logic as bulk_ingest.py).
    Returns a dict with: code, code_type, description, setting, sample_price, price_payer
    Handles both CSV (pandas Series) and JSON (dict) formats.
    """
    mapped = {}
    extraction_errors = []
    format_type = config.get('format_type', 'tall')
    
    # Get available column names (works for both pandas Series and dict)
    if isinstance(row, pd.Series):
        available_cols = set(row.index)
        is_json = False
    elif isinstance(row, dict):
        available_cols = set(row.keys())
        is_json = (format_type == 'json')
    else:
        available_cols = set()
        is_json = False
    
    # Extract code
    code_ext = config.get('code_extraction', {})
    columns = code_ext.get('columns', [])
    
    if not columns:
        # Fallback to old-style
        code_col = config.get('code_column', 'code|1')
        type_col = config.get('code_type_column')
        
        code_val = safe_get_value(row, code_col)
        # Ensure code_val is a scalar (not a Series) before boolean checks
        if isinstance(code_val, pd.Series):
            code_val = code_val.iloc[0] if len(code_val) > 0 else None
        if code_val is not None:
            code, code_type = extract_code_from_value(code_val)
            if not code:
                code = str(code_val).strip() if code_val else 'UNKNOWN'
            mapped['code'] = code
            if not code_type and type_col:
                type_val = safe_get_value(row, type_col)
                code_type = str(type_val).strip() if type_val else 'UNKNOWN'
            mapped['code_type'] = code_type or 'UNKNOWN'
        else:
            mapped['code'] = 'UNKNOWN'
            mapped['code_type'] = 'UNKNOWN'
            extraction_errors.append(f"Code column '{code_col}' not found or empty")
    else:
        # Try each code column in priority order
        code_found = False
        for col in columns:
            if col not in available_cols:
                continue
            
            code_val = safe_get_value(row, col)
            if code_val is None:
                continue
            
            # Try to extract code (handles JSON structures)
            code, code_type = extract_code_from_value(code_val)
            if not code:
                code = str(code_val).strip()
            
            if code and code != 'nan' and code != '':
                mapped['code'] = code
                
                # Try to get type from corresponding type column
                type_columns = code_ext.get('type_columns', [])
                idx = columns.index(col)
                if type_columns and idx < len(type_columns):
                    type_col = type_columns[idx]
                    if type_col in available_cols:
                        type_val = safe_get_value(row, type_col)
                        if type_val is not None:
                            code_type = str(type_val).strip()
                
                mapped['code_type'] = code_type or 'UNKNOWN'
                code_found = True
                break
        
        if not code_found:
            mapped['code'] = 'UNKNOWN'
            mapped['code_type'] = 'UNKNOWN'
            extraction_errors.append(f"Code columns {columns} not found or empty in available columns")
    
    # Extract description
    desc_col = config.get('description_column', 'description')
    desc_val = safe_get_value(row, desc_col)
    if desc_val is not None:
        mapped['description'] = str(desc_val).strip()[:100]
    else:
        mapped['description'] = 'No Description'
        extraction_errors.append(f"Description column '{desc_col}' not found")
    
    # Extract setting
    setting_ext = config.get('setting_extraction', {})
    primary = setting_ext.get('primary', 'setting')
    fallback = setting_ext.get('fallback', 'billing_class')
    
    setting_val = None
    
    # Handle JSON format - setting might be nested in standard_charges
    if is_json:
        # First try top-level
        if primary:
            setting_val = safe_get_value(row, primary)
        
        # If not found, try inside standard_charges
        if setting_val is None and 'standard_charges' in available_cols:
            sc_val = safe_get_value(row, 'standard_charges')
            sc_parsed = parse_json_value(sc_val)
            
            if isinstance(sc_parsed, list) and len(sc_parsed) > 0:
                charge_obj = sc_parsed[0]
                if isinstance(charge_obj, dict):
                    # Try primary, then fallback
                    if primary:
                        setting_val = charge_obj.get(primary)
                    if setting_val is None and fallback:
                        setting_val = charge_obj.get(fallback)
            elif isinstance(sc_parsed, dict):
                # Try primary, then fallback
                if primary:
                    setting_val = sc_parsed.get(primary)
                if setting_val is None and fallback:
                    setting_val = sc_parsed.get(fallback)
    else:
        # CSV format - direct column lookup
        setting_val = safe_get_value(row, primary)
        if setting_val is None and fallback:
            setting_val = safe_get_value(row, fallback)
    
    # Ensure setting_val is a scalar (not a Series) before boolean checks
    if isinstance(setting_val, pd.Series):
        setting_val = setting_val.iloc[0] if len(setting_val) > 0 else None
    if setting_val is not None and str(setting_val).strip() and str(setting_val) != 'nan':
        mapped['setting'] = str(setting_val).strip()
    else:
        mapped['setting'] = setting_ext.get('default', 'UNKNOWN')
    
    # Extract sample prices (multiple - like the ingestor does)
    price_ext = config.get('price_extraction', {})
    payer_style = price_ext.get('payer_style', 'column')
    
    sample_prices = []
    
    # Handle JSON format differently (nested structures)
    if is_json:
        # For JSON, standard_charges is often a list/object, not a flat column
        if 'standard_charges' in available_cols:
            sc_val = safe_get_value(row, 'standard_charges')
            sc_parsed = parse_json_value(sc_val)
            
            if isinstance(sc_parsed, list) and len(sc_parsed) > 0:
                # standard_charges is a list of charge objects
                charge_obj = sc_parsed[0]
                if isinstance(charge_obj, dict):
                    # Extract gross_charge, discounted_cash, etc.
                    if 'gross_charge' in charge_obj:
                        gross_val = charge_obj['gross_charge']
                        if gross_val is not None:
                            sample_prices.append(('GROSS', str(gross_val).strip()[:30]))
                    
                    if 'discounted_cash' in charge_obj:
                        cash_val = charge_obj['discounted_cash']
                        if cash_val is not None:
                            sample_prices.append(('CASH', str(cash_val).strip()[:30]))
                    
                    # Extract from payers_information array (insurance prices)
                    if 'payers_information' in charge_obj:
                        payers_info = charge_obj['payers_information']
                        if isinstance(payers_info, list):
                            for payer_obj in payers_info[:10]:  # Limit to first 10 payers
                                if isinstance(payer_obj, dict):
                                    payer_name = payer_obj.get('payer_name') or payer_obj.get('payer')
                                    plan_name = payer_obj.get('plan_name') or payer_obj.get('plan')
                                    estimated = payer_obj.get('estimated_amount') or payer_obj.get('negotiated_dollar')
                                    
                                    if payer_name and estimated is not None:
                                        # Format payer name with plan if available
                                        display_name = str(payer_name).strip()
                                        if plan_name:
                                            display_name = f"{display_name} ({str(plan_name).strip()[:20]})"
                                        sample_prices.append((display_name[:40], str(estimated).strip()[:30]))
                                        if len(sample_prices) >= 15:  # Allow more payers
                                            break
                    
                    # Look for other negotiated prices (fallback)
                    for key, val in charge_obj.items():
                        if key != 'payers_information' and ('negotiated' in key.lower() or 'estimated' in key.lower()) and isinstance(val, (int, float)):
                            payer_name = key.replace('_', ' ').title()[:20]
                            sample_prices.append((payer_name, str(val).strip()[:30]))
                            if len(sample_prices) >= 20:
                                break
            elif isinstance(sc_parsed, dict):
                # standard_charges is a single object
                charge_obj = sc_parsed
                if 'gross_charge' in charge_obj:
                    sample_prices.append(('GROSS', str(charge_obj['gross_charge']).strip()[:30]))
                if 'discounted_cash' in charge_obj:
                    sample_prices.append(('CASH', str(charge_obj['discounted_cash']).strip()[:30]))
                
                # Extract from payers_information array (insurance prices)
                if 'payers_information' in charge_obj:
                    payers_info = charge_obj['payers_information']
                    if isinstance(payers_info, list):
                        for payer_obj in payers_info[:10]:  # Limit to first 10 payers
                            if isinstance(payer_obj, dict):
                                payer_name = payer_obj.get('payer_name') or payer_obj.get('payer')
                                plan_name = payer_obj.get('plan_name') or payer_obj.get('plan')
                                estimated = payer_obj.get('estimated_amount') or payer_obj.get('negotiated_dollar')
                                
                                if payer_name and estimated is not None:
                                    # Format payer name with plan if available
                                    display_name = str(payer_name).strip()
                                    if plan_name:
                                        display_name = f"{display_name} ({str(plan_name).strip()[:20]})"
                                    sample_prices.append((display_name[:40], str(estimated).strip()[:30]))
                                    if len(sample_prices) >= 15:  # Allow more payers
                                        break
    else:
        # CSV format - use column names
        # Always try gross and cash first (most reliable)
        gross_col = price_ext.get('gross_column', 'standard_charge|gross')
        cash_col = price_ext.get('cash_column', 'standard_charge|discounted_cash')
        
        if gross_col and gross_col in available_cols:
            gross_val = safe_get_value(row, gross_col)
            if gross_val is not None and str(gross_val).strip() and str(gross_val) != 'nan':
                sample_prices.append(('GROSS', str(gross_val).strip()[:30]))
        
        if cash_col and cash_col in available_cols:
            cash_val = safe_get_value(row, cash_col)
            if cash_val is not None and str(cash_val).strip() and str(cash_val) != 'nan':
                sample_prices.append(('CASH', str(cash_val).strip()[:30]))
    
    # Then try to find payer prices (if header style, scan columns)
    if payer_style == 'header':
        # Scan for negotiated_dollar or estimated_amount columns
        for col in available_cols:
            if 'negotiated_dollar' in str(col) or 'estimated_amount' in str(col):
                price_val = safe_get_value(row, col)
                if price_val is not None and str(price_val).strip() and str(price_val) != 'nan':
                    # Parse payer from column name
                    parts = str(col).split('|')
                    payer = parts[1] if len(parts) > 1 else 'Unknown'
                    # Only add if it's a numeric value (not percentage)
                    try:
                        float(str(price_val).replace('$', '').replace(',', ''))
                        sample_prices.append((payer[:20], str(price_val).strip()[:30]))
                        if len(sample_prices) >= 5:  # Limit to 5 examples
                            break
                    except:
                        pass  # Skip non-numeric (like percentages)
    else:
        # Column style - try the configured price column
        price_col = price_ext.get('price_column', 'standard_charge|negotiated_dollar')
        if price_col and price_col in available_cols:
            price_val = safe_get_value(row, price_col)
            if price_val is not None and str(price_val).strip() and str(price_val) != 'nan':
                payer = safe_get_value(row, price_ext.get('payer_column', 'payer_name')) or 'Unknown'
                sample_prices.append((str(payer)[:20], str(price_val).strip()[:30]))
        else:
            # Fallback: if configured column not found, scan for price columns anyway
            # (This handles cases where payer_style is wrong in config)
            for col in available_cols:
                if ('negotiated_dollar' in str(col) or 'estimated_amount' in str(col)) and len(sample_prices) < 3:
                    price_val = safe_get_value(row, col)
                    if price_val is not None and str(price_val).strip() and str(price_val) != 'nan':
                        try:
                            # Only add if numeric (not percentage)
                            float(str(price_val).replace('$', '').replace(',', ''))
                            parts = str(col).split('|')
                            payer = parts[1] if len(parts) > 1 else 'Unknown'
                            sample_prices.append((payer[:20], str(price_val).strip()[:30]))
                            if len(sample_prices) >= 5:
                                break
                        except:
                            pass
    
    # Format sample prices for display
    if sample_prices:
        formatted_prices = []
        for payer, price in sample_prices[:3]:  # Show first 3
            try:
                # Check if it's a number
                price_float = float(str(price).replace('$', '').replace(',', ''))
                formatted_prices.append(f"{payer}: ${price_float:.2f}")
            except:
                # Not a number, show as-is
                formatted_prices.append(f"{payer}: {price}")
        
        price_display = ', '.join(formatted_prices)
        if len(sample_prices) > 3:
            price_display += f" (+{len(sample_prices) - 3} more)"
        mapped['sample_price'] = price_display
        mapped['price_count'] = len(sample_prices)
        mapped['sample_prices'] = sample_prices  # Store for payer extraction
    else:
        mapped['sample_price'] = 'N/A'
        mapped['price_count'] = 0
        mapped['sample_prices'] = []  # Store empty list for consistency
    
    mapped['price_column_used'] = f"{len(sample_prices)} prices found"
    
    # Show which raw columns are being used
    mapped['raw_code_columns'] = [c for c in columns if c in available_cols] if columns else []
    mapped['raw_desc_column'] = desc_col if desc_col in available_cols else None
    mapped['extraction_errors'] = extraction_errors
    mapped['available_columns_sample'] = list(available_cols)[:10]  # First 10 for debugging
    
    return mapped


def find_correct_header_row(csv_file, expected_columns, encoding='utf-8', max_try=5):
    """
    Try different header rows to find one that contains the expected columns.
    Returns (header_row_index, found_columns_count) or (None, 0) if not found.
    """
    for header_row in range(max_try):
        try:
            df = pd.read_csv(csv_file, header=header_row, nrows=1, dtype=str, encoding=encoding)
            found_count = sum(1 for col in expected_columns if col in df.columns)
            if found_count > 0:
                return header_row, found_count
        except:
            continue
    
    # Try with different encoding
    try:
        for header_row in range(max_try):
            try:
                df = pd.read_csv(csv_file, header=header_row, nrows=1, dtype=str, encoding='iso-8859-1')
                found_count = sum(1 for col in expected_columns if col in df.columns)
                if found_count > 0:
                    return header_row, found_count
            except:
                continue
    except:
        pass
    
    return None, 0


def get_sample_data(hospital_name, config, max_rows=5):
    """
    Load sample data and extract mapped fields according to config.
    Returns (mapped_rows, stats_dict, error_message)
    - mapped_rows: List of mapped dicts showing what will actually be ingested
    - stats_dict: Summary statistics about the data
    - error_message: Error string if something went wrong
    """
    # Use the same folder naming as download_all.py
    safe_name = sanitize_filename(hospital_name)
    hospital_dir = DOWNLOADS_DIR / safe_name
    if not hospital_dir.exists():
        return None, "Download folder not found"
    
    # Find the data file
    data_file = None
    extracted_dir = hospital_dir / "extracted"
    
    # Check extracted folder first (for ZIPs)
    if extracted_dir.exists():
        for f in extracted_dir.iterdir():
            if f.suffix.lower() in ['.csv', '.json']:
                data_file = f
                break
    
    # Check main folder
    if not data_file:
        for f in hospital_dir.iterdir():
            if f.suffix.lower() in ['.csv', '.json']:
                data_file = f
                break
    
    if not data_file:
        return None, "No data file found"
    
    try:
        if data_file.suffix.lower() == '.csv':
            # Get header row from config
            config_header_row = config.get('header_row', 0)
            encoding = config.get('encoding', 'utf-8')
            
            # Build list of expected columns
            expected_columns = []
            code_ext = config.get('code_extraction', {})
            code_cols = code_ext.get('columns', [])
            if code_cols:
                expected_columns.extend(code_cols)
            else:
                code_col = config.get('code_column')
                if code_col:
                    expected_columns.append(code_col)
            
            desc_col = config.get('description_column', 'description')
            expected_columns.append(desc_col)
            
            # Try to find correct header row if expected columns not found
            try:
                df_test = pd.read_csv(data_file, header=config_header_row, nrows=1, dtype=str, encoding=encoding)
                found_count = sum(1 for col in expected_columns if col in df_test.columns)
            except:
                found_count = 0
            
            # Auto-detect header row if expected columns not found
            header_row = config_header_row
            header_row_corrected = False
            if found_count == 0 and expected_columns:
                detected_row, detected_count = find_correct_header_row(data_file, expected_columns, encoding)
                if detected_row is not None and detected_count > found_count:
                    header_row = detected_row
                    header_row_corrected = True
            
            # Now read with (possibly corrected) header row
            try:
                df = pd.read_csv(data_file, header=header_row, nrows=max_rows * 3, 
                                dtype=str, encoding=encoding)
            except:
                df = pd.read_csv(data_file, header=header_row, nrows=max_rows * 3, 
                                dtype=str, encoding='iso-8859-1')
            
            # Add correction info to first row for display
            if header_row_corrected:
                # We'll add this info to the error messages
                pass
            
            # Read up to 20k rows for analysis (consistent across all files)
            try:
                df_analysis = pd.read_csv(data_file, header=header_row, dtype=str, encoding=encoding, nrows=20000)
            except:
                df_analysis = pd.read_csv(data_file, header=header_row, dtype=str, encoding='iso-8859-1', nrows=20000)
            
            # Extract mapped data for all analysis rows using itertuples (faster than iterrows)
            all_mapped = []
            for row_tuple in df_analysis.itertuples(index=True):
                # Convert namedtuple to dict-like access for extract_mapped_sample
                row = df_analysis.iloc[row_tuple.Index]
                mapped = extract_mapped_sample(row, config)
                
                # Add header row correction info if applicable
                if header_row_corrected and row_tuple.Index == 0:
                    mapped['extraction_errors'].append(
                        f"⚠️ Header row auto-corrected: Config says row {config_header_row}, but found columns at row {header_row}"
                    )
                
                all_mapped.append(mapped)
            
            # Calculate summary statistics
            stats = calculate_data_stats(all_mapped, config, df_analysis)
            
            # Stratified sampling: sample across different payers/codes
            payer_style = config.get('price_extraction', {}).get('payer_style', 'column')
            payer_col = config.get('price_extraction', {}).get('payer_column', 'payer_name')
            
            sampled_rows = []
            if payer_style == 'column' and payer_col and payer_col in df_analysis.columns:
                # Sample 2-3 rows per payer
                unique_payers = df_analysis[payer_col].dropna().unique()[:10]  # Limit to first 10 payers
                for payer in unique_payers:
                    payer_rows = []
                    for i, m in enumerate(all_mapped):
                        if i < len(df_analysis):
                            try:
                                row_payer_val = df_analysis.iloc[i][payer_col]
                                # Handle pandas Series/array - convert to scalar
                                if isinstance(row_payer_val, pd.Series):
                                    row_payer_val = row_payer_val.iloc[0] if len(row_payer_val) > 0 else None
                                row_payer = row_payer_val
                                # Check if payer matches (avoid array boolean ambiguity)
                                if row_payer is not None and pd.notna(row_payer):
                                    if str(row_payer).strip() == str(payer).strip():
                                        payer_rows.append(m)
                            except (IndexError, KeyError, TypeError):
                                pass
                    # Get 2-3 rows with valid codes per payer
                    payer_rows_with_codes = [r for r in payer_rows if r.get('code') != 'UNKNOWN' and r.get('code')]
                    sampled_rows.extend(payer_rows_with_codes[:3])
                    if len(sampled_rows) >= max_rows * 2:  # Get more samples for better representation
                        break
            else:
                # For header style or no payer column, sample by code diversity
                # Group by code and sample 1-2 rows per code
                code_groups = {}
                for mapped in all_mapped:
                    code = mapped.get('code', 'UNKNOWN')
                    if code != 'UNKNOWN':
                        if code not in code_groups:
                            code_groups[code] = []
                        code_groups[code].append(mapped)
                
                # Sample from different codes
                for code, rows in list(code_groups.items())[:max_rows * 2]:
                    sampled_rows.extend(rows[:2])  # 2 rows per code
                    if len(sampled_rows) >= max_rows * 2:
                        break
            
            # Fallback: if stratified sampling didn't work, use first rows with codes
            if not sampled_rows:
                sampled_rows = [m for m in all_mapped if m.get('code') != 'UNKNOWN' and m.get('code')][:max_rows * 2]
            
            # Limit to max_rows for display
            mapped_rows = sampled_rows[:max_rows]
            
            return mapped_rows if mapped_rows else None, stats, None
            
        elif data_file.suffix.lower() == '.json':
            with open(data_file, 'r') as f:
                data = json.load(f)
            
            # Handle different JSON structures
            all_records = None
            if isinstance(data, list):
                all_records = data
            elif isinstance(data, dict):
                # Look for common array keys
                for key in ['standard_charge_information', 'data', 'items', 'records']:
                    if key in data and isinstance(data[key], list):
                        all_records = data[key]
                        break
            
            if not all_records:
                return None, None, "Could not parse JSON structure"
            
            # Extract mapped data for analysis (up to 20k rows for consistency)
            records_analysis = all_records[:20000] if len(all_records) > 20000 else all_records
            all_mapped = []
            for record in records_analysis:
                if isinstance(record, dict):
                    mapped = extract_mapped_sample(record, config)
                    all_mapped.append(mapped)
            
            # Calculate summary statistics
            stats = calculate_data_stats(all_mapped, config, None)
            
            # Sample rows with valid codes (stratified if possible)
            rows_with_codes = [m for m in all_mapped if m.get('code') != 'UNKNOWN' and m.get('code')]
            
            # Group by code and sample 1-2 rows per code for diversity
            code_groups = {}
            for mapped in rows_with_codes:
                code = mapped.get('code', 'UNKNOWN')
                if code not in code_groups:
                    code_groups[code] = []
                code_groups[code].append(mapped)
            
            # Sample from different codes
            sampled_rows = []
            for code, rows in list(code_groups.items())[:max_rows * 2]:
                sampled_rows.extend(rows[:2])  # 2 rows per code
                if len(sampled_rows) >= max_rows * 2:
                    break
            
            mapped_rows = sampled_rows[:max_rows] if sampled_rows else rows_with_codes[:max_rows]
            
            return mapped_rows if mapped_rows else None, stats, None
            
    except Exception as e:
        return None, None, str(e)
    
    return None, None, "Unknown error"


def calculate_data_stats(all_mapped, config, df=None):
    """
    Calculate summary statistics about the data.
    Returns a dict with stats.
    """
    stats = {
        'total_rows_analyzed': len(all_mapped),
        'unique_codes': set(),
        'unique_payers': set(),
        'unique_settings': set(),
        'unique_code_types': set(),
        'codes_with_multiple_payers': {},  # code -> payer_count
        'rows_with_code': 0,
        'rows_with_price': 0,
        'rows_with_description': 0,
        'rows_with_setting': 0,
    }
    
    payer_style = config.get('price_extraction', {}).get('payer_style', 'column')
    payer_col = config.get('price_extraction', {}).get('payer_column', 'payer_name')
    
    # Track payer per code (for payer diversity)
    code_payer_map = {}  # code -> set of payers
    
    for idx, mapped in enumerate(all_mapped):
        code = mapped.get('code', 'UNKNOWN')
        if code != 'UNKNOWN' and code:
            stats['rows_with_code'] += 1
            stats['unique_codes'].add(code)
            stats['unique_code_types'].add(mapped.get('code_type', 'UNKNOWN'))
        
        if mapped.get('price_count', 0) > 0:
            stats['rows_with_price'] += 1
        
        if mapped.get('description', 'No Description') != 'No Description':
            stats['rows_with_description'] += 1
        
        setting = mapped.get('setting', 'UNKNOWN')
        if setting != 'UNKNOWN':
            stats['rows_with_setting'] += 1
            stats['unique_settings'].add(setting)
        
        # Track payer diversity per code
        # For CSV files: get payer from dataframe column
        if payer_style == 'column' and df is not None and payer_col and payer_col in df.columns:
            # Get payer from original dataframe using index
            if idx < len(df):
                try:
                    payer_val = df.iloc[idx][payer_col]
                    # Handle pandas Series/array - convert to scalar
                    if isinstance(payer_val, pd.Series):
                        payer_val = payer_val.iloc[0] if len(payer_val) > 0 else None
                    payer = payer_val
                    # Check if payer is valid (avoid array boolean ambiguity)
                    if payer is not None and pd.notna(payer):
                        payer_str = str(payer).strip()
                        if payer_str and payer_str != 'nan':
                            stats['unique_payers'].add(payer_str)
                            if code != 'UNKNOWN' and code:
                                if code not in code_payer_map:
                                    code_payer_map[code] = set()
                                code_payer_map[code].add(payer_str)
                except (IndexError, KeyError, TypeError):
                    pass  # Skip if index/column doesn't exist or type error
        
        # For JSON files or header-style payers: extract payers from sample_prices
        elif mapped.get('sample_prices'):
            # sample_prices is a list of tuples: [('GROSS', '3.36'), ('CASH', '2.18'), ...]
            for payer_name, _ in mapped['sample_prices']:
                if payer_name:
                    stats['unique_payers'].add(str(payer_name))
                    if code != 'UNKNOWN' and code:
                        if code not in code_payer_map:
                            code_payer_map[code] = set()
                        code_payer_map[code].add(str(payer_name))
    
    # Calculate payer diversity per code
    for code, payers in code_payer_map.items():
        if len(payers) > 1:
            stats['codes_with_multiple_payers'][code] = len(payers)
    
    # Convert sets to counts
    stats['unique_codes_count'] = len(stats['unique_codes'])
    stats['unique_payers_count'] = len(stats['unique_payers'])
    stats['unique_settings_count'] = len(stats['unique_settings'])
    stats['unique_code_types_count'] = len(stats['unique_code_types'])
    
    # Calculate percentages
    total = stats['total_rows_analyzed']
    if total > 0:
        stats['code_extraction_rate'] = (stats['rows_with_code'] / total) * 100
        stats['price_extraction_rate'] = (stats['rows_with_price'] / total) * 100
        stats['description_extraction_rate'] = (stats['rows_with_description'] / total) * 100
        stats['setting_extraction_rate'] = (stats['rows_with_setting'] / total) * 100
    else:
        stats['code_extraction_rate'] = 0
        stats['price_extraction_rate'] = 0
        stats['description_extraction_rate'] = 0
        stats['setting_extraction_rate'] = 0
    
    # Remove sets (not JSON serializable)
    del stats['unique_codes']
    del stats['unique_payers']
    del stats['unique_settings']
    del stats['unique_code_types']
    
    return stats


def slugify(name):
    """Convert hospital name to a clean slug."""
    import re
    # Remove special characters, replace spaces with underscores
    slug = re.sub(r'[^\w\s-]', '', name)
    slug = re.sub(r'[-\s]+', '_', slug)
    return slug.upper()[:50]


def generate_html(manifest):
    """Generate the preview cards HTML page."""
    
    configs = manifest.get("configs", {})
    
    # Count stats
    total = len(configs)
    validated = sum(1 for c in configs.values() if c.get("validated") == True)
    rejected = sum(1 for c in configs.values() if c.get("validated") == False)
    pending = total - validated - rejected
    
    # Sort: pending first, then rejected, then validated
    def sort_key(item):
        status = item[1].get("validated")
        if status is None:
            return (0, item[1].get("name", ""))
        elif status == False:
            return (1, item[1].get("name", ""))
        else:
            return (2, item[1].get("name", ""))
    
    sorted_configs = sorted(configs.items(), key=sort_key)
    
    # Generate cards HTML
    cards_html = []
    
    completed_configs = [(hid, info) for hid, info in sorted_configs if info.get("status") == "completed"]
    total_to_process = len(completed_configs)
    
    print(f"Processing {total_to_process} hospital cards...")
    
    for idx, (hospital_id, info) in enumerate(completed_configs, 1):
        name = info.get("name", "Unknown")
        
        # Progress indicator
        if idx % 10 == 0 or idx == total_to_process:
            print(f"  [{idx}/{total_to_process}] Processing: {name[:50]}")
        
        config = load_config(hospital_id)
        profile = load_profile(hospital_id)
        
        if not config:
            continue
        
        # Validation status
        validated = info.get("validated")
        if validated == True:
            status_class = "validated"
            status_text = "✅ Approved"
        elif validated == False:
            status_class = "rejected"
            status_text = "❌ Rejected"
        else:
            status_class = "pending"
            status_text = "⏳ Pending Review"
        
        # Config summary
        format_type = config.get("format_type", "unknown")
        confidence = config.get("confidence", "N/A")
        
        # Code extraction info
        code_ext = config.get("code_extraction", {})
        code_cols = code_ext.get("columns", [config.get("code_column", "N/A")])
        
        # Price extraction info
        price_ext = config.get("price_extraction", {})
        payer_style = price_ext.get("payer_style", price_ext.get("type", "N/A"))
        
        # Description column
        desc_col = config.get("description_column", "N/A")
        
        # Profile info
        total_rows = "N/A"
        if profile:
            total_rows = profile.get("total_rows") or profile.get("total_records") or "N/A"
            if isinstance(total_rows, int):
                total_rows = f"{total_rows:,}"
        
        # Sample data (lazy load - only try if folder exists to avoid slow errors)
        sample_data, sample_stats, sample_error = None, None, None
        safe_name = sanitize_filename(name)
        hospital_dir = DOWNLOADS_DIR / safe_name
        if hospital_dir.exists():
            sample_data, sample_stats, sample_error = get_sample_data(name, config)
        else:
            sample_error = "Download folder not found"
        
        sample_html = ""
        if sample_error:
            sample_html = f'<p class="error">Error loading sample: {html.escape(sample_error)}</p>'
        elif sample_data:
            # Show data coverage dashboard first
            if sample_stats:
                stats_html = f'''
            <div class="data-coverage-dashboard">
                <h4>📊 Data Coverage Summary (from {sample_stats.get("total_rows_analyzed", 0):,} rows analyzed)</h4>
                <div class="stats-grid">
                    <div class="stat-box">
                        <div class="stat-label">Unique Codes</div>
                        <div class="stat-value">{sample_stats.get("unique_codes_count", 0):,}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Unique Payers</div>
                        <div class="stat-value">{sample_stats.get("unique_payers_count", 0):,}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Unique Settings</div>
                        <div class="stat-value">{sample_stats.get("unique_settings_count", 0):,}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Code Types</div>
                        <div class="stat-value">{sample_stats.get("unique_code_types_count", 0):,}</div>
                    </div>
                </div>
                <div class="extraction-rates">
                    <div class="rate-item">
                        <span class="rate-label">Code Extraction:</span>
                        <span class="rate-value">{sample_stats.get("code_extraction_rate", 0):.1f}%</span>
                        <span class="rate-count">({sample_stats.get("rows_with_code", 0):,} rows)</span>
                    </div>
                    <div class="rate-item">
                        <span class="rate-label">Price Extraction:</span>
                        <span class="rate-value">{sample_stats.get("price_extraction_rate", 0):.1f}%</span>
                        <span class="rate-count">({sample_stats.get("rows_with_price", 0):,} rows)</span>
                    </div>
                    <div class="rate-item">
                        <span class="rate-label">Description Extraction:</span>
                        <span class="rate-value">{sample_stats.get("description_extraction_rate", 0):.1f}%</span>
                        <span class="rate-count">({sample_stats.get("rows_with_description", 0):,} rows)</span>
                    </div>
                    <div class="rate-item">
                        <span class="rate-label">Setting Extraction:</span>
                        <span class="rate-value">{sample_stats.get("setting_extraction_rate", 0):.1f}%</span>
                        <span class="rate-count">({sample_stats.get("rows_with_setting", 0):,} rows)</span>
                    </div>
                </div>
            </div>
                '''
                sample_html += stats_html
            
            # Show mapped data (what will actually be ingested)
            sample_html += '''
            <div class="mapped-data-info">
                <p class="info-note">📋 <strong>Mapped Data Preview</strong> - This shows what will be extracted and ingested:</p>
            </div>
            <div class="sample-table-wrapper">
                <table class="sample-table">
                    <thead>
                        <tr>
                            <th>Code</th>
                            <th>Code Type</th>
                            <th>Description</th>
                            <th>Setting</th>
                            <th>Sample Prices</th>
                            <th>Count</th>
                        </tr>
                    </thead>
                    <tbody>'''
            
            for row in sample_data[:5]:
                code = html.escape(str(row.get('code', 'N/A'))[:20])
                code_type = html.escape(str(row.get('code_type', 'N/A'))[:15])
                desc = html.escape(str(row.get('description', 'N/A'))[:60])
                setting = html.escape(str(row.get('setting', 'N/A'))[:20])
                price = html.escape(str(row.get('sample_price', 'N/A'))[:80])
                price_count = row.get('price_count', 0)
                
                # Show payer diversity if available
                payer_diversity = ""
                if sample_stats and code != "UNKNOWN" and code != "N/A":
                    payer_count = sample_stats.get('codes_with_multiple_payers', {}).get(code, 0)
                    if payer_count > 0:
                        payer_diversity = f'<br><span class="payer-diversity">📊 Appears with {payer_count} different payers</span>'
                
                # Highlight UNKNOWN codes
                code_class = "code-unknown" if code == "UNKNOWN" else ""
                code_type_class = "code-type-unknown" if code_type == "UNKNOWN" else ""
                
                sample_html += f'''
                        <tr class="{code_class}">
                            <td><strong class="{code_class}">{code}</strong>{payer_diversity}</td>
                            <td><span class="code-type {code_type_class}">{code_type}</span></td>
                            <td>{desc}</td>
                            <td>{setting}</td>
                            <td class="sample-price-cell">{price}</td>
                            <td class="price-count">{price_count}</td>
                        </tr>'''
            
            # Show extraction errors if any
            all_errors = []
            for row in sample_data[:5]:
                errors = row.get('extraction_errors', [])
                all_errors.extend(errors)
            
            if all_errors:
                unique_errors = list(set(all_errors))[:3]  # Show first 3 unique errors
                sample_html += f'''
                    </tbody>
                </table>
            </div>
            <div class="extraction-warnings">
                <p class="warning-title">⚠️ Extraction Issues Detected:</p>
                <ul>'''
                for err in unique_errors:
                    sample_html += f'<li>{html.escape(err)}</li>'
                sample_html += '</ul></div>'
            else:
                sample_html += '</tbody></table></div>'
            
            sample_html += '''
                    </tbody>
                </table>
            </div>
            
            <details class="raw-columns-info">
                <summary>🔍 Raw Column Mapping (click to see which raw columns are used)</summary>
                <div class="column-mapping">'''
            
            # Show column mapping info from first row
            if sample_data:
                first_row = sample_data[0]
                code_cols = first_row.get('raw_code_columns', [])
                desc_col = first_row.get('raw_desc_column')
                available_cols = first_row.get('available_columns_sample', [])
                
                sample_html += '<p><strong>Code columns (expected):</strong> '
                expected_cols = code_ext.get('columns', [])
                if expected_cols:
                    sample_html += ', '.join([f'<code>{html.escape(c)}</code>' for c in expected_cols])
                else:
                    sample_html += 'None specified'
                sample_html += '</p>'
                
                sample_html += '<p><strong>Code columns (found):</strong> '
                if code_cols:
                    sample_html += ', '.join([f'<code>{html.escape(c)}</code>' for c in code_cols])
                else:
                    sample_html += '<span class="error-text">Not found</span>'
                sample_html += '</p>'
                
                sample_html += f'<p><strong>Description column:</strong> '
                if desc_col:
                    sample_html += f'<code>{html.escape(desc_col)}</code>'
                else:
                    sample_html += '<span class="error-text">Not found</span>'
                sample_html += '</p>'
                
                if available_cols:
                    sample_html += f'<p><strong>Available columns (sample):</strong> '
                    sample_html += ', '.join([f'<code>{html.escape(str(c))}</code>' for c in available_cols[:10]])
                    sample_html += '...</p>'
            
            sample_html += '''
                </div>
            </details>'''
        else:
            sample_html = '<p class="muted">No sample data available (could not extract mapped fields)</p>'
        
        # Suggested hospital_id slug
        suggested_slug = slugify(name)
        
        card_html = f'''
        <div class="card {status_class}" data-hospital-id="{hospital_id}">
            <div class="card-header">
                <h3>{html.escape(name)}</h3>
                <span class="status-badge {status_class}">{status_text}</span>
            </div>
            
            <div class="card-body">
                <div class="config-summary">
                    <div class="config-row">
                        <span class="label">Format:</span>
                        <span class="value format-{format_type}">{format_type.upper()}</span>
                    </div>
                    <div class="config-row">
                        <span class="label">Total Rows:</span>
                        <span class="value">{total_rows}</span>
                    </div>
                    <div class="config-row">
                        <span class="label">Code Columns:</span>
                        <span class="value code-cols">{html.escape(str(code_cols[:3]))}</span>
                    </div>
                    <div class="config-row">
                        <span class="label">Description:</span>
                        <span class="value">{html.escape(str(desc_col))}</span>
                    </div>
                    <div class="config-row">
                        <span class="label">Payer Style:</span>
                        <span class="value">{html.escape(str(payer_style))}</span>
                    </div>
                    <div class="config-row">
                        <span class="label">AI Confidence:</span>
                        <span class="value confidence">{confidence}</span>
                    </div>
                    <div class="config-row">
                        <span class="label">Suggested ID:</span>
                        <span class="value slug">{suggested_slug}</span>
                    </div>
                </div>
                
                <details class="sample-section">
                    <summary>📊 Sample Data (click to expand)</summary>
                    {sample_html}
                </details>
                
                <details class="config-json">
                    <summary>🔧 Full Config JSON</summary>
                    <pre>{html.escape(json.dumps(config, indent=2))}</pre>
                </details>
            </div>
            
            <div class="card-actions">
                <button class="btn btn-approve" onclick="approve('{hospital_id}')">✅ Approve</button>
                <button class="btn btn-reject" onclick="reject('{hospital_id}')">❌ Reject</button>
                <button class="btn btn-edit" onclick="editConfig('{hospital_id}')">✏️ Edit Config</button>
            </div>
        </div>
        '''
        cards_html.append(card_html)
    
    # Full HTML page
    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Hospital Config Preview - Validation Dashboard</title>
    <style>
        :root {{
            --bg-dark: #0d1117;
            --bg-card: #161b22;
            --bg-hover: #21262d;
            --border: #30363d;
            --text: #c9d1d9;
            --text-muted: #8b949e;
            --accent-green: #238636;
            --accent-red: #da3633;
            --accent-yellow: #d29922;
            --accent-blue: #388bfd;
        }}
        
        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: var(--bg-dark);
            color: var(--text);
            line-height: 1.6;
            padding: 20px;
        }}
        
        .header {{
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background: var(--bg-card);
            border-radius: 12px;
            border: 1px solid var(--border);
        }}
        
        .header h1 {{
            font-size: 2rem;
            margin-bottom: 10px;
        }}
        
        .stats {{
            display: flex;
            justify-content: center;
            gap: 30px;
            margin-top: 15px;
        }}
        
        .stat {{
            text-align: center;
        }}
        
        .stat-value {{
            font-size: 2rem;
            font-weight: bold;
        }}
        
        .stat-label {{
            color: var(--text-muted);
            font-size: 0.9rem;
        }}
        
        .stat.pending .stat-value {{ color: var(--accent-yellow); }}
        .stat.validated .stat-value {{ color: var(--accent-green); }}
        .stat.rejected .stat-value {{ color: var(--accent-red); }}
        
        .filters {{
            display: flex;
            justify-content: center;
            gap: 10px;
            margin-bottom: 20px;
        }}
        
        .filter-btn {{
            padding: 8px 16px;
            border: 1px solid var(--border);
            background: var(--bg-card);
            color: var(--text);
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s;
        }}
        
        .filter-btn:hover, .filter-btn.active {{
            background: var(--accent-blue);
            border-color: var(--accent-blue);
        }}
        
        .cards-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(500px, 1fr));
            gap: 20px;
            max-width: 1600px;
            margin: 0 auto;
        }}
        
        .card {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
            transition: all 0.2s;
        }}
        
        .card:hover {{
            border-color: var(--accent-blue);
        }}
        
        .card.validated {{
            border-left: 4px solid var(--accent-green);
        }}
        
        .card.rejected {{
            border-left: 4px solid var(--accent-red);
        }}
        
        .card.pending {{
            border-left: 4px solid var(--accent-yellow);
        }}
        
        .card-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 15px 20px;
            background: var(--bg-hover);
            border-bottom: 1px solid var(--border);
        }}
        
        .card-header h3 {{
            font-size: 1.1rem;
            font-weight: 600;
        }}
        
        .status-badge {{
            padding: 4px 10px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 500;
        }}
        
        .status-badge.validated {{ background: var(--accent-green); }}
        .status-badge.rejected {{ background: var(--accent-red); }}
        .status-badge.pending {{ background: var(--accent-yellow); color: #000; }}
        
        .card-body {{
            padding: 15px 20px;
        }}
        
        .config-summary {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 8px;
        }}
        
        .config-row {{
            display: flex;
            justify-content: space-between;
            padding: 4px 0;
        }}
        
        .config-row .label {{
            color: var(--text-muted);
            font-size: 0.85rem;
        }}
        
        .config-row .value {{
            font-weight: 500;
            font-size: 0.85rem;
        }}
        
        .format-tall {{ color: var(--accent-blue); }}
        .format-wide {{ color: var(--accent-green); }}
        .format-json {{ color: var(--accent-yellow); }}
        
        .sample-section, .config-json {{
            margin-top: 15px;
            border: 1px solid var(--border);
            border-radius: 8px;
        }}
        
        .sample-section summary, .config-json summary {{
            padding: 10px 15px;
            cursor: pointer;
            background: var(--bg-hover);
            font-weight: 500;
        }}
        
        .sample-table-wrapper {{
            overflow-x: auto;
            padding: 10px;
        }}
        
        .sample-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.75rem;
        }}
        
        .sample-table th, .sample-table td {{
            padding: 6px 8px;
            border: 1px solid var(--border);
            text-align: left;
            max-width: 150px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
        
        .sample-table th {{
            background: var(--bg-hover);
            font-weight: 600;
        }}
        
        .mapped-data-info {{
            padding: 10px;
            background: var(--bg-hover);
            border-radius: 6px;
            margin-bottom: 10px;
        }}
        
        .info-note {{
            margin: 0;
            color: var(--text);
            font-size: 0.85rem;
        }}
        
        .code-type {{
            background: var(--accent-blue);
            color: white;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 0.7rem;
            font-weight: 500;
        }}
        
        .code-unknown {{
            background: var(--bg-hover);
        }}
        
        .code-unknown strong {{
            color: var(--accent-yellow);
            font-style: italic;
        }}
        
        .code-type-unknown {{
            background: var(--accent-yellow);
            color: #000;
        }}
        
        .extraction-warnings {{
            margin-top: 10px;
            padding: 10px;
            background: rgba(218, 54, 51, 0.1);
            border-left: 3px solid var(--accent-red);
            border-radius: 4px;
        }}
        
        .warning-title {{
            margin: 0 0 8px 0;
            font-weight: 600;
            color: var(--accent-red);
        }}
        
        .extraction-warnings ul {{
            margin: 0;
            padding-left: 20px;
        }}
        
        .extraction-warnings li {{
            margin: 4px 0;
            font-size: 0.85rem;
        }}
        
        .error-text {{
            color: var(--accent-red);
            font-weight: 500;
        }}
        
        .sample-price-cell {{
            font-size: 0.75rem;
            max-width: 200px;
        }}
        
        .price-count {{
            text-align: center;
            font-weight: 600;
            color: var(--accent-blue);
        }}
        
        .data-coverage-dashboard {{
            margin: 15px 0;
            padding: 15px;
            background: var(--bg-hover);
            border-radius: 8px;
            border: 1px solid var(--border);
        }}
        
        .data-coverage-dashboard h4 {{
            margin: 0 0 15px 0;
            font-size: 0.9rem;
            color: var(--text);
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 10px;
            margin-bottom: 15px;
        }}
        
        .stat-box {{
            background: var(--bg-dark);
            padding: 10px;
            border-radius: 6px;
            text-align: center;
            border: 1px solid var(--border);
        }}
        
        .stat-label {{
            font-size: 0.75rem;
            color: var(--text-muted);
            margin-bottom: 5px;
        }}
        
        .stat-value {{
            font-size: 1.2rem;
            font-weight: 600;
            color: var(--accent-blue);
        }}
        
        .extraction-rates {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 8px;
        }}
        
        .rate-item {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 6px 10px;
            background: var(--bg-dark);
            border-radius: 4px;
            font-size: 0.8rem;
        }}
        
        .rate-label {{
            color: var(--text-muted);
        }}
        
        .rate-value {{
            font-weight: 600;
            color: var(--accent-green);
        }}
        
        .rate-count {{
            color: var(--text-muted);
            font-size: 0.75rem;
        }}
        
        .payer-diversity {{
            font-size: 0.7rem;
            color: var(--accent-yellow);
            font-style: italic;
        }}
        
        .raw-columns-info {{
            margin-top: 10px;
            border: 1px solid var(--border);
            border-radius: 6px;
        }}
        
        .raw-columns-info summary {{
            padding: 8px 12px;
            cursor: pointer;
            background: var(--bg-hover);
            font-size: 0.85rem;
        }}
        
        .column-mapping {{
            padding: 12px;
            background: var(--bg-dark);
        }}
        
        .column-mapping p {{
            margin: 8px 0;
            font-size: 0.85rem;
        }}
        
        .column-mapping code {{
            background: var(--bg-hover);
            padding: 2px 6px;
            border-radius: 4px;
            font-family: monospace;
            font-size: 0.8rem;
            color: var(--accent-blue);
        }}
        
        .config-json pre {{
            padding: 15px;
            overflow-x: auto;
            font-size: 0.75rem;
            background: var(--bg-dark);
            max-height: 300px;
        }}
        
        .card-actions {{
            display: flex;
            gap: 10px;
            padding: 15px 20px;
            background: var(--bg-hover);
            border-top: 1px solid var(--border);
        }}
        
        .btn {{
            flex: 1;
            padding: 10px 15px;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-weight: 500;
            transition: all 0.2s;
        }}
        
        .btn-approve {{
            background: var(--accent-green);
            color: white;
        }}
        
        .btn-reject {{
            background: var(--accent-red);
            color: white;
        }}
        
        .btn-edit {{
            background: var(--border);
            color: var(--text);
        }}
        
        .btn:hover {{
            opacity: 0.9;
            transform: translateY(-1px);
        }}
        
        .error {{
            color: var(--accent-red);
            padding: 10px;
        }}
        
        .muted {{
            color: var(--text-muted);
            padding: 10px;
        }}
        
        .hidden {{
            display: none !important;
        }}
        
        .toast {{
            position: fixed;
            bottom: 20px;
            right: 20px;
            padding: 15px 25px;
            background: var(--accent-green);
            color: white;
            border-radius: 8px;
            font-weight: 500;
            transform: translateY(100px);
            opacity: 0;
            transition: all 0.3s;
            z-index: 1000;
        }}
        
        .toast.show {{
            transform: translateY(0);
            opacity: 1;
        }}
        
        .toast.error {{
            background: var(--accent-red);
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🏥 Hospital Config Validation Dashboard</h1>
        <p>Review AI-generated configs before bulk ingestion</p>
        <div class="stats">
            <div class="stat pending">
                <div class="stat-value" id="pending-count">{pending}</div>
                <div class="stat-label">Pending</div>
            </div>
            <div class="stat validated">
                <div class="stat-value" id="validated-count">{validated}</div>
                <div class="stat-label">Approved</div>
            </div>
            <div class="stat rejected">
                <div class="stat-value" id="rejected-count">{rejected}</div>
                <div class="stat-label">Rejected</div>
            </div>
            <div class="stat">
                <div class="stat-value">{total}</div>
                <div class="stat-label">Total</div>
            </div>
        </div>
    </div>
    
    <div class="filters">
        <button class="filter-btn active" onclick="filterCards('all')">All</button>
        <button class="filter-btn" onclick="filterCards('pending')">⏳ Pending</button>
        <button class="filter-btn" onclick="filterCards('validated')">✅ Approved</button>
        <button class="filter-btn" onclick="filterCards('rejected')">❌ Rejected</button>
    </div>
    
    <div class="cards-grid">
        {"".join(cards_html)}
    </div>
    
    <div class="toast" id="toast"></div>
    
    <script>
        const API_BASE = 'http://localhost:{SERVER_PORT}';
        
        function showToast(message, isError = false) {{
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.className = 'toast show' + (isError ? ' error' : '');
            setTimeout(() => toast.className = 'toast', 3000);
        }}
        
        async function approve(hospitalId) {{
            try {{
                const response = await fetch(`${{API_BASE}}/api/validate/${{hospitalId}}?status=approved`);
                const data = await response.json();
                if (data.success) {{
                    updateCardStatus(hospitalId, 'validated');
                    showToast('✅ Hospital approved!');
                    updateStats();
                }}
            }} catch (e) {{
                showToast('Error: ' + e.message, true);
            }}
        }}
        
        async function reject(hospitalId) {{
            try {{
                const response = await fetch(`${{API_BASE}}/api/validate/${{hospitalId}}?status=rejected`);
                const data = await response.json();
                if (data.success) {{
                    updateCardStatus(hospitalId, 'rejected');
                    showToast('❌ Hospital rejected');
                    updateStats();
                }}
            }} catch (e) {{
                showToast('Error: ' + e.message, true);
            }}
        }}
        
        function editConfig(hospitalId) {{
            window.open(`${{API_BASE}}/api/config/${{hospitalId}}`, '_blank');
        }}
        
        function updateCardStatus(hospitalId, status) {{
            const card = document.querySelector(`[data-hospital-id="${{hospitalId}}"]`);
            if (card) {{
                card.className = `card ${{status}}`;
                const badge = card.querySelector('.status-badge');
                if (status === 'validated') {{
                    badge.textContent = '✅ Approved';
                    badge.className = 'status-badge validated';
                }} else if (status === 'rejected') {{
                    badge.textContent = '❌ Rejected';
                    badge.className = 'status-badge rejected';
                }}
            }}
        }}
        
        function updateStats() {{
            const cards = document.querySelectorAll('.card');
            let pending = 0, validated = 0, rejected = 0;
            cards.forEach(card => {{
                if (card.classList.contains('validated')) validated++;
                else if (card.classList.contains('rejected')) rejected++;
                else pending++;
            }});
            document.getElementById('pending-count').textContent = pending;
            document.getElementById('validated-count').textContent = validated;
            document.getElementById('rejected-count').textContent = rejected;
        }}
        
        function filterCards(filter) {{
            document.querySelectorAll('.filter-btn').forEach(btn => btn.classList.remove('active'));
            event.target.classList.add('active');
            
            document.querySelectorAll('.card').forEach(card => {{
                if (filter === 'all') {{
                    card.classList.remove('hidden');
                }} else {{
                    if (card.classList.contains(filter)) {{
                        card.classList.remove('hidden');
                    }} else {{
                        card.classList.add('hidden');
                    }}
                }}
            }});
        }}
    </script>
</body>
</html>
'''
    
    return html_content


class ValidationHandler(SimpleHTTPRequestHandler):
    """HTTP handler for the validation API."""
    
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        
        # Serve the HTML preview
        if path == '/' or path == '/index.html':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            with open(PREVIEW_HTML, 'rb') as f:
                self.wfile.write(f.read())
            return
        
        # API: Validate a hospital
        if path.startswith('/api/validate/'):
            hospital_id = path.split('/')[-1]
            status = query.get('status', [''])[0]
            
            manifest = load_config_manifest()
            
            if hospital_id in manifest.get('configs', {}):
                if status == 'approved':
                    manifest['configs'][hospital_id]['validated'] = True
                    manifest['configs'][hospital_id]['validated_at'] = datetime.now().isoformat()
                elif status == 'rejected':
                    manifest['configs'][hospital_id]['validated'] = False
                    manifest['configs'][hospital_id]['rejected_at'] = datetime.now().isoformat()
                
                save_config_manifest(manifest)
                
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'success': True}).encode())
            else:
                self.send_response(404)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Hospital not found'}).encode())
            return
        
        # API: Get config JSON
        if path.startswith('/api/config/'):
            hospital_id = path.split('/')[-1]
            config = load_config(hospital_id)
            
            if config:
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps(config, indent=2).encode())
            else:
                self.send_response(404)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': 'Config not found'}).encode())
            return
        
        # Default: 404
        self.send_response(404)
        self.end_headers()
    
    def log_message(self, format, *args):
        # Suppress logging for cleaner output
        pass


def main():
    """Main entry point."""
    print("=" * 60)
    print("  PREVIEW CARD GENERATOR - Phase 4")
    print("=" * 60)
    
    # Load manifest
    manifest = load_config_manifest()
    configs = manifest.get("configs", {})
    
    total = len(configs)
    completed = sum(1 for c in configs.values() if c.get("status") == "completed")
    validated = sum(1 for c in configs.values() if c.get("validated") == True)
    
    print(f"\nTotal configs: {total}")
    print(f"Completed: {completed}")
    print(f"Already validated: {validated}")
    
    # Generate HTML
    print("\nGenerating preview cards...")
    html_content = generate_html(manifest)
    
    with open(PREVIEW_HTML, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Preview saved to: {PREVIEW_HTML}")
    
    # Start server
    print(f"\n🌐 Starting validation server on http://localhost:{SERVER_PORT}")
    print("   Open this URL in your browser to review configs")
    print("   Press Ctrl+C to stop the server\n")
    
    try:
        server = HTTPServer(('localhost', SERVER_PORT), ValidationHandler)
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n\n⚠️  Server stopped")
        
        # Show final stats
        manifest = load_config_manifest()
        validated = sum(1 for c in manifest.get("configs", {}).values() if c.get("validated") == True)
        rejected = sum(1 for c in manifest.get("configs", {}).values() if c.get("validated") == False)
        
        print(f"\nFinal validation status:")
        print(f"  ✅ Approved: {validated}")
        print(f"  ❌ Rejected: {rejected}")
        print(f"  ⏳ Pending: {total - validated - rejected}")


if __name__ == "__main__":
    main()

