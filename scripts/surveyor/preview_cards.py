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
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

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


# All extraction functions are now imported from extractors.py


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
    
    # Extract code using shared extractor
    code, code_type = extract_code(row, config, is_json)
    mapped['code'] = code
    mapped['code_type'] = code_type
    
    # Get code columns for debugging/display (always needed, not just on error)
    code_ext = config.get('code_extraction', {})
    columns = code_ext.get('columns', [])
    if not columns:
        columns = [config.get('code_column', 'code|1')]
    
    if code == 'UNKNOWN':
        extraction_errors.append(f"Code columns {columns} not found or empty in available columns")
    
    # Extract description
    desc_col = config.get('description_column', 'description')
    desc_val = safe_get_value(row, desc_col)
    if desc_val is not None:
        mapped['description'] = str(desc_val).strip()[:100]
    else:
        mapped['description'] = 'No Description'
        extraction_errors.append(f"Description column '{desc_col}' not found")
    
    # Extract setting using shared extractor
    mapped['setting'] = extract_setting(row, config, is_json)
    
    # Extract prices using unified PriceExtractor
    price_extractor = PriceExtractor(config, row, available_cols, is_json)
    sample_prices = price_extractor.extract_all_prices()
    total_price_count = len(sample_prices)  # All prices are counted
    
    # Format sample prices for display
    # Use total_price_count (which should equal sample_prices length now since we removed all limits)
    # But keep the check in case total_price_count wasn't updated in some paths
    if 'total_price_count' in locals() and total_price_count >= len(sample_prices):
        actual_price_count = total_price_count
    else:
        actual_price_count = len(sample_prices)
    
    if sample_prices:
        formatted_prices = []
        for price_info in sample_prices[:3]:  # Show first 3
            # Handle both dict and tuple formats (backward compatibility)
            if isinstance(price_info, dict):
                payer = price_info.get('payer', 'Unknown')
                amount = price_info.get('amount')
                percentage = price_info.get('percentage')
                methodology = price_info.get('methodology')
                
                if amount:
                    try:
                        price_float = float(str(amount).replace('$', '').replace(',', ''))
                        formatted_prices.append(f"{payer}: ${price_float:.2f}")
                    except:
                        formatted_prices.append(f"{payer}: {amount}")
                elif percentage:
                    # Percentage-based pricing
                    pct_str = f"{payer}: {percentage}%"
                    if methodology:
                        pct_str += f" ({methodology})"
                    formatted_prices.append(pct_str)
                else:
                    formatted_prices.append(f"{payer}: N/A")
            else:
                # Legacy tuple format: (payer, price)
                payer, price = price_info
                try:
                    price_float = float(str(price).replace('$', '').replace(',', ''))
                    formatted_prices.append(f"{payer}: ${price_float:.2f}")
                except:
                    formatted_prices.append(f"{payer}: {price}")
        
        price_display = ', '.join(formatted_prices)
        if actual_price_count > 3:
            price_display += f" (+{actual_price_count - 3} more)"
        mapped['sample_price'] = price_display
        mapped['price_count'] = actual_price_count  # Use actual count, not just sample_prices length
        mapped['sample_prices'] = sample_prices  # Store for payer extraction
    else:
        mapped['sample_price'] = 'N/A'
        mapped['price_count'] = 0
        mapped['sample_prices'] = []  # Store empty list for consistency
    
    mapped['price_column_used'] = f"{len(sample_prices)} prices found"
    
    # For JSON files: Extract payers from payers_information during mapping (single-pass optimization)
    extracted_payers = set()
    if is_json and 'standard_charges' in available_cols:
        sc_val = safe_get_value(row, 'standard_charges')
        sc_parsed = parse_json_value(sc_val)
        
        if isinstance(sc_parsed, list):
            for charge_obj in sc_parsed:
                if isinstance(charge_obj, dict) and 'payers_information' in charge_obj:
                    payers_info = charge_obj['payers_information']
                    if isinstance(payers_info, list):
                        for payer_obj in payers_info:
                            if isinstance(payer_obj, dict):
                                payer_name = payer_obj.get('payer_name') or payer_obj.get('payer')
                                if payer_name:
                                    extracted_payers.add(str(payer_name).strip())
        elif isinstance(sc_parsed, dict) and 'payers_information' in sc_parsed:
            payers_info = sc_parsed['payers_information']
            if isinstance(payers_info, list):
                for payer_obj in payers_info:
                    if isinstance(payer_obj, dict):
                        payer_name = payer_obj.get('payer_name') or payer_obj.get('payer')
                        if payer_name:
                            extracted_payers.add(str(payer_name).strip())
        
        # Also add GROSS/CASH if present
        if isinstance(sc_parsed, (list, dict)):
            charge_obj = sc_parsed[0] if isinstance(sc_parsed, list) else sc_parsed
            if isinstance(charge_obj, dict):
                if 'gross_charge' in charge_obj:
                    extracted_payers.add('GROSS')
                if 'discounted_cash' in charge_obj:
                    extracted_payers.add('CASH')
    
    # Store extracted payers for JSON files (used for stats calculation)
    if is_json:
        mapped['extracted_payers'] = extracted_payers
    
    # Show which raw columns are being used
    mapped['raw_code_columns'] = [c for c in columns if c in available_cols] if columns else []
    mapped['raw_desc_column'] = desc_col if desc_col in available_cols else None
    mapped['extraction_errors'] = extraction_errors
    mapped['available_columns_sample'] = list(available_cols)[:10]  # First 10 for debugging
    
    return mapped


# Header row fallback logic removed - Phase 2 should detect header_row correctly


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
            
            # Read CSV with header row from config (Phase 2 should have detected it correctly)
            # Read small sample for display
            try:
                df = pd.read_csv(data_file, header=config_header_row, nrows=max_rows * 3, 
                                dtype=str, encoding=encoding)
            except:
                df = pd.read_csv(data_file, header=config_header_row, nrows=max_rows * 3, 
                                dtype=str, encoding='iso-8859-1')
            
            # OPTIMIZATION 1: Process entire file in chunks (10k rows at a time)
            # This avoids loading entire file into memory and is much faster
            chunk_size = 10000
            all_mapped = []
            all_unique_codes = set()
            all_unique_payers = set()
            all_unique_settings = set()
            all_unique_code_types = set()
            rows_with_code = 0
            rows_with_price = 0
            rows_with_description = 0
            rows_with_setting = 0
            code_payer_map = {}  # For payer diversity tracking
            sample_rows_by_code = {}  # For stratified sampling
            total_rows_processed = 0
            
            payer_style = config.get('price_extraction', {}).get('payer_style', 'column')
            payer_col = config.get('price_extraction', {}).get('payer_column', 'payer_name')
            
            print(f"  Processing CSV in chunks of {chunk_size:,} rows...")
            
            try:
                chunk_reader = pd.read_csv(data_file, header=config_header_row, dtype=str, 
                                         encoding=encoding, chunksize=chunk_size)
            except:
                chunk_reader = pd.read_csv(data_file, header=config_header_row, dtype=str, 
                                         encoding='iso-8859-1', chunksize=chunk_size)
            
            for chunk_idx, df_chunk in enumerate(chunk_reader):
                # OPTIMIZATION 3: Use itertuples with index, then access chunk by position
                # Reset index to ensure 0-based positions for iloc access
                df_chunk = df_chunk.reset_index(drop=True)
                # Use enumerate to get position, itertuples for fast iteration
                for row_pos, row_tuple in enumerate(df_chunk.itertuples(index=False)):
                    # Access row from chunk by position (faster than creating new Series)
                    row = df_chunk.iloc[row_pos]
                    
                    mapped = extract_mapped_sample(row, config)
                    
                    # Aggregate stats as we go (avoid storing all mapped data)
                    code = mapped.get('code', 'UNKNOWN')
                    if code != 'UNKNOWN' and code:
                        all_unique_codes.add(code)
                        rows_with_code += 1
                        all_unique_code_types.add(mapped.get('code_type', 'UNKNOWN'))
                        
                        # Store sample rows for stratified sampling
                        if code not in sample_rows_by_code:
                            sample_rows_by_code[code] = []
                        if len(sample_rows_by_code[code]) < 3:  # Keep max 3 per code
                            sample_rows_by_code[code].append(mapped)
                    
                    if mapped.get('price_count', 0) > 0:
                        rows_with_price += 1
                    
                    if mapped.get('description', 'No Description') != 'No Description':
                        rows_with_description += 1
                    
                    setting = mapped.get('setting', 'UNKNOWN')
                    if setting != 'UNKNOWN':
                        rows_with_setting += 1
                        all_unique_settings.add(setting)
                    
                    # Track payers per code for diversity
                    if payer_style == 'column' and payer_col and payer_col in df_chunk.columns:
                        payer_val = row.get(payer_col)
                        if payer_val is not None and pd.notna(payer_val):
                            # Handle Series conversion
                            if isinstance(payer_val, pd.Series):
                                payer_val = payer_val.iloc[0] if len(payer_val) > 0 else None
                            if payer_val is not None:
                                payer_str = str(payer_val).strip()
                                if payer_str and payer_str != 'nan':
                                    all_unique_payers.add(payer_str)
                                    if code != 'UNKNOWN' and code:
                                        if code not in code_payer_map:
                                            code_payer_map[code] = set()
                                        code_payer_map[code].add(payer_str)
                    elif payer_style == 'header' and mapped.get('sample_prices'):
                        for price_info in mapped['sample_prices']:
                            # Handle both dict and tuple formats
                            if isinstance(price_info, dict):
                                payer_name = price_info.get('payer')
                            else:
                                payer_name = price_info[0] if len(price_info) > 0 else None
                            
                            if payer_name:
                                all_unique_payers.add(str(payer_name))
                                if code != 'UNKNOWN' and code:
                                    if code not in code_payer_map:
                                        code_payer_map[code] = set()
                                    code_payer_map[code].add(str(payer_name))
                
                total_rows_processed += len(df_chunk)
                
                # Progress indicator for large files
                if chunk_idx % 10 == 0 and chunk_idx > 0:
                    print(f"    Processed {total_rows_processed:,} rows...")
            
            # Build stats dict from aggregated data
            stats = {
                'total_rows_analyzed': total_rows_processed,
                'unique_codes_count': len(all_unique_codes),
                'unique_payers_count': len(all_unique_payers),
                'unique_settings_count': len(all_unique_settings),
                'unique_code_types_count': len(all_unique_code_types),
                'rows_with_code': rows_with_code,
                'rows_with_price': rows_with_price,
                'rows_with_description': rows_with_description,
                'rows_with_setting': rows_with_setting,
                'code_extraction_rate': (rows_with_code / total_rows_processed * 100) if total_rows_processed > 0 else 0,
                'price_extraction_rate': (rows_with_price / total_rows_processed * 100) if total_rows_processed > 0 else 0,
                'description_extraction_rate': (rows_with_description / total_rows_processed * 100) if total_rows_processed > 0 else 0,
                'setting_extraction_rate': (rows_with_setting / total_rows_processed * 100) if total_rows_processed > 0 else 0,
                'codes_with_multiple_payers': {code: len(payers) for code, payers in code_payer_map.items() if len(payers) > 1}
            }
            
            # For header-style payers: scan columns from first chunk to get payer list
            if payer_style == 'header':
                try:
                    first_chunk = pd.read_csv(data_file, header=config_header_row, dtype=str, 
                                            encoding=encoding, nrows=1)
                except:
                    first_chunk = pd.read_csv(data_file, header=config_header_row, dtype=str, 
                                            encoding='iso-8859-1', nrows=1)
                
                for col in first_chunk.columns:
                    col_str = str(col)
                    if ('negotiated_dollar' in col_str or 'estimated_amount' in col_str) and '|' in col_str:
                        parts = col_str.split('|')
                        if len(parts) >= 2:
                            payer_name = parts[1].strip()
                            if payer_name and payer_name.lower() not in ['gross', 'discounted_cash', 'min', 'max', 'negotiated_dollar', 'estimated_amount']:
                                all_unique_payers.add(payer_name)
                    elif 'gross' in col_str.lower() and '|' in col_str:
                        parts = col_str.split('|')
                        if len(parts) >= 2 and parts[1].strip().lower() == 'gross':
                            all_unique_payers.add('GROSS')
                    elif 'discounted_cash' in col_str.lower() or ('cash' in col_str.lower() and 'discounted' in col_str.lower()):
                        all_unique_payers.add('CASH')
                
                stats['unique_payers_count'] = len(all_unique_payers)
            
            # Use sample_rows_by_code for stratified sampling
            all_mapped = []  # Rebuild from samples for display
            
            # Stratified sampling from sample_rows_by_code (already collected during chunked processing)
            sampled_rows = []
            for code, rows in list(sample_rows_by_code.items())[:max_rows * 2]:
                sampled_rows.extend(rows)
                if len(sampled_rows) >= max_rows * 2:
                    break
            
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
            
            # OPTIMIZATION 2: Single-pass JSON processing - extract payers during mapping
            print(f"  Processing {len(all_records):,} JSON records...")
            all_mapped = []
            all_json_payers = set()  # Aggregate payers from all records
            all_unique_codes = set()
            all_unique_settings = set()
            all_unique_code_types = set()
            rows_with_code = 0
            rows_with_price = 0
            rows_with_description = 0
            rows_with_setting = 0
            code_payer_map = {}  # For payer diversity tracking
            sample_rows_by_code = {}  # For stratified sampling
            
            # Process all records (no 20k limit)
            for record in all_records:
                if isinstance(record, dict):
                    mapped = extract_mapped_sample(record, config)
                    all_mapped.append(mapped)
                    
                    # OPTIMIZATION: Payers already extracted in extract_mapped_sample
                    # Just aggregate them here
                    if 'extracted_payers' in mapped:
                        all_json_payers.update(mapped['extracted_payers'])
                    
                    # Aggregate stats as we go
                    code = mapped.get('code', 'UNKNOWN')
                    if code != 'UNKNOWN' and code:
                        all_unique_codes.add(code)
                        rows_with_code += 1
                        all_unique_code_types.add(mapped.get('code_type', 'UNKNOWN'))
                        
                        # Store sample rows for stratified sampling
                        if code not in sample_rows_by_code:
                            sample_rows_by_code[code] = []
                        if len(sample_rows_by_code[code]) < 3:  # Keep max 3 per code
                            sample_rows_by_code[code].append(mapped)
                    
                    if mapped.get('price_count', 0) > 0:
                        rows_with_price += 1
                    
                    if mapped.get('description', 'No Description') != 'No Description':
                        rows_with_description += 1
                    
                    setting = mapped.get('setting', 'UNKNOWN')
                    if setting != 'UNKNOWN':
                        rows_with_setting += 1
                        all_unique_settings.add(setting)
                    
                    # Track payer diversity per code from extracted payers
                    if 'extracted_payers' in mapped:
                        for payer_name in mapped['extracted_payers']:
                            if code != 'UNKNOWN' and code:
                                if code not in code_payer_map:
                                    code_payer_map[code] = set()
                                code_payer_map[code].add(str(payer_name))
                    
                    # Progress indicator for large files
                    if len(all_mapped) % 50000 == 0:
                        print(f"    Processed {len(all_mapped):,} records...")
            
            # Build stats dict from aggregated data
            stats = {
                'total_rows_analyzed': len(all_mapped),
                'unique_codes_count': len(all_unique_codes),
                'unique_payers_count': len(all_json_payers),
                'unique_settings_count': len(all_unique_settings),
                'unique_code_types_count': len(all_unique_code_types),
                'rows_with_code': rows_with_code,
                'rows_with_price': rows_with_price,
                'rows_with_description': rows_with_description,
                'rows_with_setting': rows_with_setting,
                'code_extraction_rate': (rows_with_code / len(all_mapped) * 100) if all_mapped else 0,
                'price_extraction_rate': (rows_with_price / len(all_mapped) * 100) if all_mapped else 0,
                'description_extraction_rate': (rows_with_description / len(all_mapped) * 100) if all_mapped else 0,
                'setting_extraction_rate': (rows_with_setting / len(all_mapped) * 100) if all_mapped else 0,
                'codes_with_multiple_payers': {code: len(payers) for code, payers in code_payer_map.items() if len(payers) > 1}
            }
            
            # Stratified sampling from sample_rows_by_code (already collected during processing)
            sampled_rows = []
            for code, rows in list(sample_rows_by_code.items())[:max_rows * 2]:
                sampled_rows.extend(rows)
                if len(sampled_rows) >= max_rows * 2:
                    break
            
            mapped_rows = sampled_rows[:max_rows] if sampled_rows else []
            
            return mapped_rows if mapped_rows else None, stats, None
            
    except Exception as e:
        return None, None, str(e)
    
    return None, None, "Unknown error"


def calculate_data_stats(all_mapped, config, df=None, json_payers=None):
    """
    Calculate summary statistics about the data.
    Returns a dict with stats.
    
    Args:
        all_mapped: List of mapped data dictionaries
        config: Configuration dictionary
        df: DataFrame (for CSV files) or None (for JSON files)
        json_payers: Set of unique payer names extracted from JSON records (for JSON files)
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
    format_type = config.get('format_type', 'tall')
    
    # Track payer per code (for payer diversity)
    code_payer_map = {}  # code -> set of payers
    
    # For JSON files: Use the comprehensive payer list from scanning all records
    if format_type == 'json' and df is None and json_payers:
        # Add all payers found from scanning all records' payers_information arrays
        for payer_name in json_payers:
            stats['unique_payers'].add(payer_name)
    
    # For header-style payers: scan ALL payer columns in dataframe to get complete payer list
    if payer_style == 'header' and df is not None:
        # Scan all columns to find payer columns (columns with negotiated_dollar or estimated_amount)
        payer_columns = []
        for col in df.columns:
            col_str = str(col)
            if ('negotiated_dollar' in col_str or 'estimated_amount' in col_str) and '|' in col_str:
                parts = col_str.split('|')
                if len(parts) >= 2:
                    payer_name = parts[1].strip()
                    # Exclude common non-payer values
                    if payer_name and payer_name.lower() not in ['gross', 'discounted_cash', 'min', 'max', 'negotiated_dollar', 'estimated_amount']:
                        if payer_name not in [p[0] for p in payer_columns]:
                            payer_columns.append((payer_name, col))
        
        # Also check for GROSS and CASH columns
        for col in df.columns:
            col_str = str(col)
            if 'gross' in col_str.lower() and '|' in col_str:
                parts = col_str.split('|')
                if len(parts) >= 2 and parts[1].strip().lower() == 'gross':
                    payer_columns.append(('GROSS', col))
            elif 'discounted_cash' in col_str.lower() or ('cash' in col_str.lower() and 'discounted' in col_str.lower()):
                parts = col_str.split('|')
                if len(parts) >= 2:
                    payer_columns.append(('CASH', col))
        
        # Extract unique payers from all payer columns
        for payer_name, col_name in payer_columns:
            stats['unique_payers'].add(payer_name)
    
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
        
        # For header-style payers: also track payers per code from sample_prices (for diversity tracking)
        elif payer_style == 'header' and mapped.get('sample_prices'):
            # sample_prices is now a list of dicts: [{'payer': 'GROSS', 'amount': '3.36', ...}, ...]
            for price_info in mapped['sample_prices']:
                # Handle both dict and tuple formats
                if isinstance(price_info, dict):
                    payer_name = price_info.get('payer')
                else:
                    payer_name = price_info[0] if len(price_info) > 0 else None
                
                if payer_name and code != 'UNKNOWN' and code:
                    if code not in code_payer_map:
                        code_payer_map[code] = set()
                    code_payer_map[code].add(str(payer_name))
        
        # For JSON files: extract payers from sample_prices (fallback if json_payers not provided)
        elif df is None and mapped.get('sample_prices') and not json_payers:
            # sample_prices is now a list of dicts
            for price_info in mapped['sample_prices']:
                # Handle both dict and tuple formats
                if isinstance(price_info, dict):
                    payer_name = price_info.get('payer')
                else:
                    payer_name = price_info[0] if len(price_info) > 0 else None
                
                if payer_name:
                    stats['unique_payers'].add(str(payer_name))
                    if code != 'UNKNOWN' and code:
                        if code not in code_payer_map:
                            code_payer_map[code] = set()
                        code_payer_map[code].add(str(payer_name))
        
        # For JSON files with json_payers: also track payer diversity per code from sample_prices
        elif df is None and json_payers and mapped.get('sample_prices'):
            for price_info in mapped['sample_prices']:
                # Handle both dict and tuple formats
                if isinstance(price_info, dict):
                    payer_name = price_info.get('payer')
                else:
                    payer_name = price_info[0] if len(price_info) > 0 else None
                
                if payer_name and code != 'UNKNOWN' and code:
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


def process_single_hospital_card(args):
    """
    Process a single hospital card (for parallel execution).
    Returns (hospital_id, card_html) or (hospital_id, None) if failed.
    """
    hospital_id, info = args
    
    try:
        name = info.get("name", "Unknown")
        config = load_config(hospital_id)
        profile = load_profile(hospital_id)
        
        if not config:
            return (hospital_id, None)
        
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
        
        # Generate sample HTML (same logic as before)
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
                code_cols_found = first_row.get('raw_code_columns', [])
                desc_col_found = first_row.get('raw_desc_column')
                available_cols = first_row.get('available_columns_sample', [])
                
                sample_html += '<p><strong>Code columns (expected):</strong> '
                expected_cols = code_ext.get('columns', [])
                if expected_cols:
                    sample_html += ', '.join([f'<code>{html.escape(c)}</code>' for c in expected_cols])
                else:
                    sample_html += 'None specified'
                sample_html += '</p>'
                
                sample_html += '<p><strong>Code columns (found):</strong> '
                if code_cols_found:
                    sample_html += ', '.join([f'<code>{html.escape(c)}</code>' for c in code_cols_found])
                else:
                    sample_html += '<span class="error-text">Not found</span>'
                sample_html += '</p>'
                
                sample_html += f'<p><strong>Description column:</strong> '
                if desc_col_found:
                    sample_html += f'<code>{html.escape(desc_col_found)}</code>'
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
        
        return (hospital_id, card_html)
    
    except Exception as e:
        # Return error card
        error_card = f'''
        <div class="card pending" data-hospital-id="{hospital_id}">
            <div class="card-header">
                <h3>{html.escape(info.get("name", "Unknown"))}</h3>
                <span class="status-badge pending">⏳ Error</span>
            </div>
            <div class="card-body">
                <p class="error">Error processing: {html.escape(str(e))}</p>
            </div>
        </div>
        '''
        return (hospital_id, error_card)


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
    print(f"Using parallel processing with {multiprocessing.cpu_count()} workers...")
    
    # Process hospitals in parallel
    cards_dict = {}  # hospital_id -> card_html
    num_workers = min(12, multiprocessing.cpu_count())  # Use 12 workers or CPU count, whichever is less
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        future_to_hospital = {
            executor.submit(process_single_hospital_card, (hospital_id, info)): hospital_id
            for hospital_id, info in completed_configs
        }
        
        # Process completed tasks as they finish
        completed_count = 0
        for future in as_completed(future_to_hospital):
            hospital_id = future_to_hospital[future]
            completed_count += 1
            
            try:
                result_id, card_html = future.result()
                if card_html:
                    cards_dict[result_id] = card_html
                
                # Progress update every 10 completions
                if completed_count % 10 == 0 or completed_count == total_to_process:
                    print(f"  [{completed_count}/{total_to_process}] Completed processing...")
            except Exception as e:
                print(f"  ⚠️  Error processing {hospital_id}: {e}")
                # Create error card
                error_card = f'''
                <div class="card pending" data-hospital-id="{hospital_id}">
                    <div class="card-header">
                        <h3>Error</h3>
                        <span class="status-badge pending">⏳ Error</span>
                    </div>
                    <div class="card-body">
                        <p class="error">Error: {html.escape(str(e))}</p>
                    </div>
                </div>
                '''
                cards_dict[hospital_id] = error_card
    
    # Build cards_html in the same order as completed_configs
    for hospital_id, info in completed_configs:
        if hospital_id in cards_dict:
            cards_html.append(cards_dict[hospital_id])
    
    print(f"✅ Processed {len(cards_html)} hospital cards")
    
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
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate preview cards and optionally start validation server")
    parser.add_argument(
        '--no-server',
        action='store_true',
        help='Generate HTML but do not start the validation server'
    )
    args = parser.parse_args()
    
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
    
    # Start server (unless --no-server flag is set)
    if args.no_server:
        print(f"\n💡 Preview HTML generated. To start the server, run:")
        print(f"   python3 {Path(__file__).name}")
    else:
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

