#!/usr/bin/env python3
"""
Phase 2: Deep CSV Analyzer

Thoroughly analyzes downloaded hospital files to generate statistical profiles.
Supports: CSV, JSON, ZIP files
Features:
- Full file scanning (not just headers)
- Column type detection
- Pattern recognition (codes, prices, descriptions)
- Format detection (tall vs wide)
- Header row detection
- ZIP extraction support
- Deep JSON structure analysis
"""

import json
import os
import sys
import re
import csv
import zipfile
from pathlib import Path
from datetime import datetime
from collections import Counter
import statistics
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Configuration
DATA_DIR = Path(__file__).parent.parent.parent / "data"
DOWNLOADS_DIR = DATA_DIR / "downloads"
PROFILES_DIR = DATA_DIR / "profiles"
MANIFEST_FILE = DOWNLOADS_DIR / "download_manifest.json"
ANALYSIS_MANIFEST = PROFILES_DIR / "analysis_manifest.json"

# Sample size for very large files (rows to analyze)
# Note: No row/record limits - we process entire files using chunked/batched processing
SAMPLE_SIZE = 10  # Number of sample values to store
CHUNK_SIZE = 10000  # Process CSV files in chunks of 10K rows (for memory efficiency)
JSON_BATCH_SIZE = 10000  # Process JSON records in batches of 10K (for memory efficiency, but processes ALL records)


def format_number(value, default="N/A"):
    """Safely format a number with commas, handling non-numeric values."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return f"{value:,}"
    return str(value)


def sanitize_filename(name):
    """Create a safe directory/file name (matches download_all.py)."""
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
    return safe[:100]  # Limit length


def extract_zip_file(zip_path):
    """
    Extract a ZIP file to a subdirectory.
    Returns the path to the extracted directory and list of data files found.
    """
    zip_path = Path(zip_path)
    extract_dir = zip_path.parent / "extracted"
    
    # Check if already extracted
    if extract_dir.exists():
        # Find existing data files
        data_files = list(extract_dir.glob("**/*.csv")) + list(extract_dir.glob("**/*.json"))
        if data_files:
            return extract_dir, data_files
    
    # Extract ZIP
    extract_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Extract only data files
            for name in zf.namelist():
                # Skip directories and non-data files
                if name.endswith('/'):
                    continue
                if not any(name.lower().endswith(ext) for ext in ['.csv', '.json', '.xlsx', '.xls']):
                    continue
                
                # Extract to flat structure (avoid nested folders)
                filename = Path(name).name
                target_path = extract_dir / filename
                
                with zf.open(name) as source, open(target_path, 'wb') as target:
                    target.write(source.read())
        
        # Find extracted data files
        data_files = list(extract_dir.glob("*.csv")) + list(extract_dir.glob("*.json"))
        return extract_dir, data_files
        
    except Exception as e:
        return None, []


def load_manifest():
    """Load download manifest to find completed downloads."""
    if not MANIFEST_FILE.exists():
        print(f"ERROR: Download manifest not found at {MANIFEST_FILE}")
        print("Please run download_all.py first.")
        sys.exit(1)
    
    with open(MANIFEST_FILE, 'r') as f:
        return json.load(f)


def load_analysis_manifest():
    """Load or create analysis manifest."""
    if ANALYSIS_MANIFEST.exists():
        with open(ANALYSIS_MANIFEST, 'r') as f:
            return json.load(f)
    return {
        "created": datetime.now().isoformat(),
        "last_updated": None,
        "analyses": {},
        "stats": {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0
        }
    }


def save_analysis_manifest(manifest):
    """Save analysis manifest."""
    manifest["last_updated"] = datetime.now().isoformat()
    with open(ANALYSIS_MANIFEST, 'w') as f:
        json.dump(manifest, f, indent=2)


def detect_encoding(file_path):
    """
    Detect file encoding by trying to decode the ENTIRE file.
    This ensures we catch encoding issues anywhere in the file, not just the beginning.
    """
    # Read entire file as binary (fast - ~0.05s for 55MB file)
    with open(file_path, 'rb') as f:
        data = f.read()
    
    # Try common encodings in order of likelihood
    encodings = ['utf-8', 'iso-8859-1', 'cp1252', 'latin1', 'utf-8-sig']
    
    for encoding in encodings:
        try:
            data.decode(encoding)
            return encoding  # First encoding that successfully decodes entire file
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    # If all fail, return utf-8 (will use encoding_errors='replace' in pandas as last resort)
    return 'utf-8'


def detect_header_row(rows, max_check=10):
    """
    Detect which row contains the header.
    Returns the 0-based index of the header row.
    """
    if not rows:
        return 0
    
    # Heuristics:
    # 1. Header row typically has mostly text (column names)
    # 2. Data rows typically have numbers/mixed content
    # 3. Header row often has unique values
    
    scores = []
    
    for i, row in enumerate(rows[:max_check]):
        if not row:
            scores.append(-1)
            continue
            
        score = 0
        
        # Check for common header patterns
        row_lower = [str(c).lower() for c in row]
        header_keywords = ['code', 'description', 'price', 'charge', 'payer', 'plan', 
                          'type', 'name', 'amount', 'rate', 'billing', 'standard']
        
        for cell in row_lower:
            for keyword in header_keywords:
                if keyword in cell:
                    score += 5
        
        # Penalize rows with too many numeric values
        numeric_count = sum(1 for c in row if is_likely_numeric(str(c)))
        score -= numeric_count * 2
        
        # Bonus for rows where all values are non-empty strings
        non_empty = sum(1 for c in row if str(c).strip())
        if non_empty == len(row) and len(row) > 3:
            score += 3
            
        scores.append(score)
    
    if not scores:
        return 0
    
    # Return row with highest score
    max_score = max(scores)
    return scores.index(max_score)


def is_likely_numeric(value):
    """Check if a value looks like a number."""
    if not value or not str(value).strip():
        return False
    
    clean = str(value).strip().replace('$', '').replace(',', '').replace('%', '')
    try:
        float(clean)
        return True
    except ValueError:
        return False


def is_likely_price(value):
    """Check if a value looks like a price."""
    if not value:
        return False
    
    val_str = str(value).strip()
    
    # Check for currency patterns
    if '$' in val_str:
        return True
    
    # Check for decimal numbers in typical price ranges
    if is_likely_numeric(val_str):
        try:
            num = float(val_str.replace('$', '').replace(',', ''))
            # Prices typically between 0.01 and 10,000,000
            if 0.01 <= num <= 10000000:
                return True
        except:
            pass
    
    return False


def is_likely_code(value):
    """Check if a value looks like a medical billing code."""
    if not value:
        return False
    
    val_str = str(value).strip()
    
    # CPT: 5 digits
    if re.match(r'^\d{5}$', val_str):
        return True
    
    # HCPCS: Letter + 4 digits
    if re.match(r'^[A-Za-z]\d{4}$', val_str):
        return True
    
    # ICD-10: Letter + digits + optional decimal
    if re.match(r'^[A-Za-z]\d{2,3}\.?\d*$', val_str):
        return True
    
    # DRG: 3 digits
    if re.match(r'^\d{3}$', val_str):
        return True
    
    # NDC: 10-11 digits with dashes
    if re.match(r'^\d{4,5}-\d{3,4}-\d{1,2}$', val_str):
        return True
    
    # Revenue code: 4 digits
    if re.match(r'^\d{4}$', val_str):
        return True
    
    return False


def analyze_column(values, column_name):
    """
    Analyze a single column's values.
    Returns a dictionary of statistics.
    """
    total = len(values)
    non_empty = [v for v in values if v is not None and str(v).strip()]
    
    # Basic stats
    fill_rate = len(non_empty) / total if total > 0 else 0
    unique_values = set(str(v) for v in non_empty)
    unique_count = len(unique_values)
    
    # Type detection
    numeric_count = sum(1 for v in non_empty if is_likely_numeric(v))
    price_count = sum(1 for v in non_empty if is_likely_price(v))
    code_count = sum(1 for v in non_empty if is_likely_code(v))
    
    # Determine likely type
    if len(non_empty) == 0:
        likely_type = "empty"
    elif price_count / len(non_empty) > 0.5:
        likely_type = "price"
    elif code_count / len(non_empty) > 0.5:
        likely_type = "code"
    elif numeric_count / len(non_empty) > 0.8:
        likely_type = "numeric"
    else:
        likely_type = "text"
    
    # Get sample values (diverse selection)
    sample_values = []
    if non_empty:
        # Get first few, last few, and some random middle values
        unique_list = list(unique_values)[:SAMPLE_SIZE * 2]
        sample_values = unique_list[:SAMPLE_SIZE]
    
    # Numeric stats if applicable
    numeric_stats = None
    if likely_type in ["price", "numeric"] and numeric_count > 0:
        try:
            nums = []
            for v in non_empty:
                try:
                    clean = str(v).replace('$', '').replace(',', '')
                    nums.append(float(clean))
                except:
                    pass
            
            if nums:
                numeric_stats = {
                    "min": min(nums),
                    "max": max(nums),
                    "mean": statistics.mean(nums),
                    "median": statistics.median(nums) if len(nums) > 1 else nums[0]
                }
        except:
            pass
    
    # Pattern detection for column name
    # IMPORTANT: Order matters! More specific patterns must come first
    col_lower = column_name.lower()
    inferred_purpose = "unknown"
    
    # Check for payer/insurance FIRST (before "name" which could match "payer_name")
    if any(k in col_lower for k in ['payer', 'insurance', 'carrier']):
        inferred_purpose = "payer"
    elif any(k in col_lower for k in ['plan', 'product']):
        inferred_purpose = "plan"
    elif any(k in col_lower for k in ['code', 'cpt', 'hcpcs', 'icd', 'drg', 'ndc']):
        inferred_purpose = "code"
    # Check for description, but exclude columns that are clearly payer-related
    elif any(k in col_lower for k in ['desc', 'procedure', 'service']) or ('name' in col_lower and 'payer' not in col_lower and 'plan' not in col_lower):
        inferred_purpose = "description"
    # Check for category/setting indicators BEFORE price (to catch "COST CENTER", "DEPARTMENT", etc.)
    elif any(k in col_lower for k in ['center', 'department', 'location']) or 'cost center' in col_lower:
        inferred_purpose = "category"
    elif any(k in col_lower for k in ['type', 'class', 'category', 'setting']):
        inferred_purpose = "category"
    elif any(k in col_lower for k in ['charge', 'price', 'amount', 'rate', 'dollar', 'cost']):
        inferred_purpose = "price"
    elif any(k in col_lower for k in ['note', 'comment', 'additional', 'modifier']):
        inferred_purpose = "notes"
    elif any(k in col_lower for k in ['gross', 'cash', 'discounted']):
        inferred_purpose = "standard_charge"
    elif any(k in col_lower for k in ['negotiated', 'contract']):
        inferred_purpose = "negotiated_rate"
    
    return {
        "column_name": column_name,
        "fill_rate": round(fill_rate, 3),
        "unique_count": unique_count,
        "total_rows": total,
        "non_empty_rows": len(non_empty),
        "likely_type": likely_type,
        "inferred_purpose": inferred_purpose,
        "sample_values": sample_values[:SAMPLE_SIZE],
        "numeric_stats": numeric_stats
    }


def detect_format_type(columns, column_analyses):
    """
    Detect if the file is 'tall' or 'wide' format.
    
    Tall: One row per payer/price combination
          Has columns like 'payer_name', 'plan_name', one price column
    
    Wide: One row per item, prices in columns
          Has many columns with payer names in them
    """
    col_names_lower = [c.lower() for c in columns]
    
    # Check for tall format indicators
    has_payer_column = any('payer' in c for c in col_names_lower)
    has_plan_column = any('plan' in c for c in col_names_lower)
    
    # Check for wide format indicators (multiple price columns with payer names)
    price_columns = [a for a in column_analyses if a['inferred_purpose'] in ['price', 'negotiated_rate', 'standard_charge']]
    
    # Count columns that look like "standard_charge|PayerName|..."
    payer_in_column = sum(1 for c in columns if '|' in c and any(k in c.lower() for k in ['charge', 'dollar', 'amount']))
    
    if has_payer_column and has_plan_column:
        return "tall"
    elif payer_in_column > 5:
        return "wide"
    elif len(price_columns) > 10:
        return "wide"
    else:
        return "tall"  # Default assumption


def generate_config_template(profile, hospital_name=None):
    """
    Generate a config template with deterministic values from Phase 2 analysis.
    Properly separates code columns from type columns.
    
    Args:
        profile: Analysis profile dictionary
        hospital_name: Optional hospital name
    
    Returns:
        Dictionary with config template structure
    """
    detected_patterns = profile.get("detected_patterns", {})
    format_type = profile.get("format_type", "tall")
    header_row = profile.get("header_row", 0)
    encoding = profile.get("encoding", "utf-8")
    
    # Separate code columns from type columns
    all_code_like_cols = detected_patterns.get("code_columns", [])
    code_only_columns = []
    type_only_columns = []
    
    for col in all_code_like_cols:
        col_str = str(col)
        # Normalize spaces around pipes for consistent matching
        # "code | 1 | type" → "code|1|type" (handles spaces in column names)
        col_normalized = '|'.join(part.strip() for part in col_str.split('|'))
        
        # Check if column name suggests it's a type column
        # Check both normalized and original (for _type suffix)
        if '|type' in col_normalized.lower() or col_normalized.endswith('|type') or col_str.endswith('_type'):
            type_only_columns.append(col)
        else:
            code_only_columns.append(col)
    
    # Match type columns to code columns by name pattern
    # e.g., 'code|1' -> 'code|1|type', 'code|2' -> 'code|2|type'
    # Normalize spaces around pipes for matching
    matched_type_columns = []
    if code_only_columns and type_only_columns:
        for code_col in code_only_columns:
            code_col_str = str(code_col)
            code_col_normalized = '|'.join(part.strip() for part in code_col_str.split('|'))
            # Try to find matching type column
            matching_type = None
            for type_col in type_only_columns:
                type_col_str = str(type_col)
                type_col_normalized = '|'.join(part.strip() for part in type_col_str.split('|'))
                # Check if type column matches (e.g., 'code|1|type' matches 'code|1')
                # Check both normalized and original strings
                if (code_col_normalized in type_col_normalized or 
                    type_col_normalized.startswith(code_col_normalized.split('|')[0]) or
                    code_col_str in type_col_str or 
                    type_col_str.startswith(code_col_str.split('|')[0])):
                    matching_type = type_col
                    break
            matched_type_columns.append(matching_type)
    elif not type_only_columns:
        # No type columns found, set to None
        matched_type_columns = [None] * len(code_only_columns) if code_only_columns else None
    
    # If we have type columns but couldn't match them, use them in order
    if type_only_columns and len(matched_type_columns) < len(code_only_columns):
        for i, type_col in enumerate(type_only_columns):
            if i < len(code_only_columns):
                if i >= len(matched_type_columns):
                    matched_type_columns.append(type_col)
                elif matched_type_columns[i] is None:
                    matched_type_columns[i] = type_col
    
    # If no matches found, set to None
    if not matched_type_columns:
        matched_type_columns = None
    
    # Get other deterministic values
    description_column = detected_patterns.get("description_column")
    payer_style = detected_patterns.get("payer_style")
    payer_column = detected_patterns.get("payer_column")
    # Ensure setting_primary is never None (use "setting" as default)
    setting_primary = detected_patterns.get("setting_primary") or "setting"
    setting_fallback = detected_patterns.get("setting_fallback") or "billing_class"
    
    # Find notes column
    notes_column = None
    for col_analysis in profile.get("column_analyses", []):
        if col_analysis.get("inferred_purpose") == "notes":
            notes_column = col_analysis.get("column_name")
            break
    
    # Build config template
    template = {
        "hospital_name": hospital_name or "Unknown",
        "format_type": format_type,
        "header_row": header_row,
        "encoding": encoding,
        "code_extraction": {
            "columns": code_only_columns if code_only_columns else all_code_like_cols[:3],  # Fallback if separation failed
            "type_columns": matched_type_columns if matched_type_columns and any(matched_type_columns) else None,
            "priority": ["CPT", "HCPCS", "MS-DRG", "APR-DRG", "NDC", "CDM", "Local"],
            "auto_normalize": True
        },
        "description_column": description_column,
        "setting_extraction": {
            "primary": setting_primary,
            "fallback": setting_fallback if setting_fallback else None,
            "default": "UNKNOWN"
        },
        "price_extraction": {
            "type": format_type,
            "payer_style": payer_style,
            "payer_column": payer_column
        },
        "skip_rules": {
            "placeholder_threshold": 99999999,
            "formula_patterns": ["Formula", "algorithm"],
            "empty_code_skip": True
        },
        "notes_column": notes_column
    }
    
    return template


def analyze_csv_file(file_path):
    """
    Analyze a CSV or Excel file and return a comprehensive profile.
    Supports: CSV, XLSX, XLS files
    """
    profile = {
        "file_path": str(file_path),
        "file_name": file_path.name,
        "file_size_mb": round(file_path.stat().st_size / (1024 * 1024), 2),
        "analysis_timestamp": datetime.now().isoformat(),
        "encoding": None,
        "header_row": None,
        "total_rows": 0,
        "total_columns": 0,
        "columns": [],
        "column_analyses": [],
        "format_type": None,
        "detected_patterns": {},
        "warnings": [],
        "errors": []
    }
    
    try:
        # Detect file type
        file_ext = file_path.suffix.lower()
        is_excel = file_ext in ['.xlsx', '.xls']
        
        # Detect encoding (only for CSV files)
        encoding = None
        if not is_excel:
            encoding = detect_encoding(file_path)
            profile["encoding"] = encoding
        
        # Read first rows to detect header (different approach for CSV vs Excel)
        if is_excel:
            # For Excel: read first 16 rows using pandas
            df_preview = pd.read_excel(file_path, header=None, nrows=16)
            first_rows = df_preview.values.tolist()
        else:
            # For CSV: read first rows using csv.reader
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                reader = csv.reader(f)
                first_rows = []
                for i, row in enumerate(reader):
                    first_rows.append(row)
                    if i >= 15:  # Read first 16 rows for header detection
                        break
        
        # Detect header row
        header_row_idx = detect_header_row(first_rows)
        profile["header_row"] = header_row_idx
        
        if header_row_idx >= len(first_rows):
            profile["errors"].append("Could not detect header row")
            return profile
        
        columns = [str(c) for c in first_rows[header_row_idx]]  # Convert to strings
        profile["columns"] = columns
        profile["total_columns"] = len(columns)
        
        # Process entire file in chunks using pandas (matches preview_cards.py approach)
        # This allows us to analyze all rows without loading everything into memory
        print(f"    Processing entire file in chunks of {CHUNK_SIZE:,} rows...")
        
        # Initialize column value collectors (we'll aggregate across chunks)
        column_value_collectors = {col_idx: [] for col_idx in range(len(columns))}
        total_rows = 0
        
        try:
            # Use pandas chunked reader (supports both CSV and Excel)
            if is_excel:
                # Excel files: read in chunks (pandas doesn't support chunksize for Excel, so we'll read all)
                # For very large Excel files, we'll need to read all at once (Excel format limitation)
                df_full = pd.read_excel(file_path, header=header_row_idx, dtype=str)
                total_rows = len(df_full)
                
                # Process in chunks manually for consistency
                for chunk_start in range(0, total_rows, CHUNK_SIZE):
                    chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
                    df_chunk = df_full.iloc[chunk_start:chunk_end]
                    
                    # Collect values for each column from this chunk
                    for col_idx, col_name in enumerate(columns):
                        if col_name in df_chunk.columns:
                            col_values_chunk = df_chunk[col_name].fillna('').astype(str).tolist()
                            column_value_collectors[col_idx].extend(col_values_chunk)
                        else:
                            column_value_collectors[col_idx].extend([None] * len(df_chunk))
                    
                    # Progress indicator
                    if chunk_end % (CHUNK_SIZE * 10) == 0:
                        print(f"      Processed {chunk_end:,} of {total_rows:,} rows...")
            else:
                # CSV files: use pandas chunked reader
                chunk_reader = pd.read_csv(
                    file_path,
                    header=header_row_idx,
                    dtype=str,
                    encoding=encoding,
                    chunksize=CHUNK_SIZE,
                    on_bad_lines='skip'  # Skip malformed rows
                )
                
                for chunk_idx, df_chunk in enumerate(chunk_reader):
                    chunk_rows = len(df_chunk)
                    total_rows += chunk_rows
                    
                    # Collect values for each column from this chunk
                    for col_idx, col_name in enumerate(columns):
                        if col_name in df_chunk.columns:
                            # Get all values from this column in this chunk
                            col_values_chunk = df_chunk[col_name].fillna('').astype(str).tolist()
                            column_value_collectors[col_idx].extend(col_values_chunk)
                        else:
                            # Column missing in this chunk - add None values
                            column_value_collectors[col_idx].extend([None] * chunk_rows)
                    
                    # Progress indicator for very large files
                    if chunk_idx % 10 == 0 and chunk_idx > 0:
                        print(f"      Processed {total_rows:,} rows...")
            
            profile["total_rows"] = total_rows
            
        except Exception as e:
            # Fallback to csv.reader if pandas fails
            print(f"    Warning: Pandas chunked reading failed ({e}), falling back to csv.reader...")
            # Open new file handle (the previous 'f' was closed when 'with' block ended)
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                reader = csv.reader(f)
                
                # Skip to header
                for _ in range(header_row_idx + 1):
                    next(reader, None)
                
                # Read all data rows
                data_rows = []
                row_count = 0
                
                for row in reader:
                    data_rows.append(row)
                    row_count += 1
                    # Progress indicator every 100k rows
                    if row_count % 100000 == 0:
                        print(f"      Processed {row_count:,} rows...")
                
                profile["total_rows"] = row_count
                
                # Collect column values from csv.reader data
                for col_idx, col_name in enumerate(columns):
                    col_values = []
                    for row in data_rows:
                        if col_idx < len(row):
                            col_values.append(row[col_idx])
                        else:
                            col_values.append(None)
                    column_value_collectors[col_idx] = col_values
        
        # Analyze each column using collected values
        for col_idx, col_name in enumerate(columns):
            col_values = column_value_collectors[col_idx]
            col_analysis = analyze_column(col_values, col_name)
            profile["column_analyses"].append(col_analysis)
        
        # Detect format type (always set, even if analysis failed partially)
        if profile["column_analyses"]:
            profile["format_type"] = detect_format_type(columns, profile["column_analyses"])
        else:
            # Fallback if no column analyses available
            profile["format_type"] = "tall"  # Default assumption
        
        # Identify key columns
        code_columns = [a["column_name"] for a in profile["column_analyses"] 
                       if a["inferred_purpose"] == "code" or a["likely_type"] == "code"]
        desc_columns = [a["column_name"] for a in profile["column_analyses"] 
                       if a["inferred_purpose"] == "description"]
        
        # Price columns: filter out empty ones and prioritize by fill rate
        # Also detect placeholder-heavy columns (e.g., all 999999999 values)
        price_column_candidates = []
        for a in profile["column_analyses"]:
            col_name = a["column_name"]
            inferred = a["inferred_purpose"]
            likely_type = a["likely_type"]
            fill_rate = a.get("fill_rate", 0)
            
            # Check if it's a price-related column
            is_price_col = (inferred in ["price", "standard_charge", "negotiated_rate"] 
                           or likely_type == "price")
            
            if is_price_col:
                # Check for placeholder values in sample data
                sample_values = a.get("sample_values", [])
                is_placeholder_heavy = False
                if sample_values and fill_rate > 0.5:  # Only check if mostly filled
                    # Check if all sample values are the same large number (placeholder)
                    try:
                        numeric_samples = []
                        for val in sample_values[:10]:  # Check up to 10 samples
                            try:
                                num = float(str(val).replace('$', '').replace(',', ''))
                                numeric_samples.append(num)
                            except:
                                pass
                        
                        if numeric_samples:
                            # If all values are the same and > 999999, likely placeholder
                            if len(set(numeric_samples)) == 1 and numeric_samples[0] >= 999999:
                                is_placeholder_heavy = True
                    except:
                        pass
                
                price_column_candidates.append({
                    "column_name": col_name,
                    "fill_rate": fill_rate,
                    "likely_type": likely_type,
                    "inferred_purpose": inferred,
                    "is_placeholder_heavy": is_placeholder_heavy,
                    "sample_values": sample_values[:5]  # Store samples for AI
                })
        
        # Filter and sort: remove empty columns, prioritize by fill rate, exclude placeholders
        price_columns_filtered = [
            c for c in price_column_candidates 
            if c["fill_rate"] > 0.01 and not c["is_placeholder_heavy"]  # At least 1% filled, not placeholders
        ]
        
        # Sort by fill rate (descending), then by column name
        price_columns_filtered.sort(key=lambda x: (-x["fill_rate"], x["column_name"]))
        
        # Extract just column names for backward compatibility
        price_columns = [c["column_name"] for c in price_columns_filtered]
        
        # Store detailed price column info for Phase 3 (AI config generation)
        price_columns_detailed = price_columns_filtered[:20]  # Top 20
        
        # Detect header-style payer format (payers embedded in column names)
        header_style_payer_columns = []
        for col in columns:
            col_str = str(col)
            # Look for columns with payer names embedded (e.g., standard_charge|Aetna|...)
            if ('negotiated_dollar' in col_str or 'estimated_amount' in col_str) and '|' in col_str:
                parts = col_str.split('|')
                if len(parts) >= 2:
                    # Second part might be payer name
                    potential_payer = parts[1].strip()
                    # Exclude common non-payer values
                    if potential_payer and potential_payer.lower() not in ['gross', 'discounted_cash', 'min', 'max', 'negotiated_dollar', 'estimated_amount']:
                        header_style_payer_columns.append(col_str)
        
        has_header_style_payers = len(header_style_payer_columns) > 0
        
        # Detect payer style (deterministic from Phase 2)
        payer_style = None
        payer_column = None
        if has_header_style_payers:
            payer_style = 'header'
            payer_column = None
        else:
            # Check for payer column
            payer_cols = [a["column_name"] for a in profile["column_analyses"] 
                         if a["inferred_purpose"] == "payer"]
            if payer_cols:
                payer_style = 'column'
                payer_column = payer_cols[0]  # Use first payer column found
        
        # Detect best description column
        description_column = None
        if desc_columns:
            # Prefer columns with "description" in name, then "desc", then others
            for col in desc_columns:
                if 'description' in col.lower():
                    description_column = col
                    break
            if not description_column:
                description_column = desc_columns[0]
        
        # Detect setting columns
        setting_primary = None
        setting_fallback = None
        setting_cols = [a["column_name"] for a in profile["column_analyses"] 
                        if a["inferred_purpose"] == "category" and 
                        ('setting' in a["column_name"].lower() or 'billing' in a["column_name"].lower())]
        for col in setting_cols:
            if 'setting' in col.lower() and not setting_primary:
                setting_primary = col
            elif 'billing' in col.lower() and not setting_fallback:
                setting_fallback = col
        
        profile["detected_patterns"] = {
            "code_columns": code_columns[:5],  # Top 5
            "description_columns": desc_columns[:3],
            "price_columns": price_columns[:20],  # Can have many in wide format (filtered and sorted)
            "price_columns_detailed": price_columns_detailed,  # Detailed info with fill rates for AI
            "has_payer_column": any(a["inferred_purpose"] == "payer" for a in profile["column_analyses"]),
            "has_plan_column": any(a["inferred_purpose"] == "plan" for a in profile["column_analyses"]),
            "has_notes_column": any(a["inferred_purpose"] == "notes" for a in profile["column_analyses"]),
            "has_header_style_payers": has_header_style_payers,
            "header_style_payer_columns": header_style_payer_columns[:10] if has_header_style_payers else [],  # Store examples
            # Deterministic values for config template
            "payer_style": payer_style,  # 'header' or 'column' or None if unclear
            "payer_column": payer_column,  # Column name if column style
            "description_column": description_column,  # Best guess
            "setting_primary": setting_primary,  # Primary setting column
            "setting_fallback": setting_fallback,  # Fallback setting column
        }
        
        # Generate config template with proper code/type column separation
        profile["config_template"] = generate_config_template(profile, hospital_name=None)
        
    except Exception as e:
        profile["errors"].append(str(e))
    
    return profile


def analyze_json_file(file_path):
    """
    Analyze a JSON file and return a comprehensive profile.
    JSON files from hospitals typically have a specific structure.
    Performs deep analysis of the standard_charge_information array.
    """
    profile = {
        "file_path": str(file_path),
        "file_name": file_path.name,
        "file_size_mb": round(file_path.stat().st_size / (1024 * 1024), 2),
        "analysis_timestamp": datetime.now().isoformat(),
        "encoding": "utf-8",
        "format_type": "json",
        "json_structure": None,
        "total_records": 0,
        "total_columns": 0,
        "columns": [],
        "column_analyses": [],
        "sample_record": None,
        "hospital_metadata": {},
        "detected_patterns": {},
        "warnings": [],
        "errors": []
    }
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Analyze structure
        if isinstance(data, list):
            profile["json_structure"] = "array_of_objects"
            profile["total_records"] = len(data)
            records = data
            
        elif isinstance(data, dict):
            profile["json_structure"] = "object"
            keys = list(data.keys())
            profile["detected_patterns"]["top_level_keys"] = keys[:20]
            
            # Extract hospital metadata
            metadata_keys = ['hospital_name', 'last_updated_on', 'version', 'hospital_location', 'hospital_address']
            for key in metadata_keys:
                if key in data:
                    profile["hospital_metadata"][key] = data[key]
            
            # Look for the charge information array
            records = None
            for key in ['standard_charge_information', 'charges', 'items', 'data']:
                if key in data and isinstance(data[key], list):
                    records = data[key]
                    profile["total_records"] = len(records)
                    break
            
            if not records:
                profile["warnings"].append("Could not find charge array in JSON structure")
                return profile
        else:
            profile["errors"].append("Unexpected JSON structure (not object or array)")
            return profile
        
        # Analyze the records structure - process ALL records (no limit)
        if records and len(records) > 0:
            total_records = len(records)
            profile["total_records"] = total_records
            
            print(f"    Processing {total_records:,} JSON records...")
            
            # Store sample record
            first_record = records[0]
            if len(str(first_record)) < 5000:
                profile["sample_record"] = first_record
            else:
                profile["sample_record"] = {"note": "Record too large, showing keys only", "keys": list(first_record.keys()) if isinstance(first_record, dict) else []}
            
            # Analyze record structure
            if isinstance(first_record, dict):
                # Get all unique keys across ALL records (check first 1000 for efficiency, then sample rest)
                all_keys = set()
                # Check first 1000 records for keys
                for record in records[:1000]:
                    if isinstance(record, dict):
                        all_keys.update(record.keys())
                
                # Sample additional records to catch keys that appear later
                if total_records > 1000:
                    # Sample from middle and end
                    step = max(1, total_records // 1000)
                    for i in range(1000, total_records, step):
                        if isinstance(records[i], dict):
                            all_keys.update(records[i].keys())
                
                profile["columns"] = list(all_keys)
                profile["total_columns"] = len(all_keys)
                
                # Analyze each "column" (key) - process ALL records in batches for memory efficiency
                for key in all_keys:
                    values = []
                    # Process in batches to avoid memory issues
                    for batch_start in range(0, total_records, JSON_BATCH_SIZE):
                        batch_end = min(batch_start + JSON_BATCH_SIZE, total_records)
                        batch_records = records[batch_start:batch_end]
                        
                        for record in batch_records:
                            if isinstance(record, dict):
                                val = record.get(key)
                                # Flatten nested structures for analysis
                                if isinstance(val, (list, dict)):
                                    values.append(str(val)[:100])  # Truncate complex values
                                else:
                                    values.append(val)
                            else:
                                values.append(None)
                        
                        # Progress indicator for very large files
                        if batch_end % (JSON_BATCH_SIZE * 10) == 0:
                            print(f"      Processed {batch_end:,} of {total_records:,} records for column analysis...")
                    
                    col_analysis = analyze_column(values, key)
                    profile["column_analyses"].append(col_analysis)
                
                # Detect patterns specific to JSON hospital files
                code_cols = [a["column_name"] for a in profile["column_analyses"] 
                            if a["inferred_purpose"] == "code" or 'code' in a["column_name"].lower()]
                desc_cols = [a["column_name"] for a in profile["column_analyses"] 
                            if a["inferred_purpose"] == "description" or 'desc' in a["column_name"].lower()]
                
                # Detect best description column (same logic as CSV files)
                description_column = None
                if desc_cols:
                    # Prefer columns with "description" in name, then "desc", then others
                    for col in desc_cols:
                        if 'description' in col.lower():
                            description_column = col
                            break
                    if not description_column:
                        description_column = desc_cols[0]
                
                # Check for nested structures common in hospital JSON
                has_code_info = 'code_information' in all_keys
                has_standard_charges = 'standard_charges' in all_keys
                has_drug_info = 'drug_information' in all_keys
                
                profile["detected_patterns"] = {
                    "code_columns": code_cols[:5],
                    "description_columns": desc_cols[:3],
                    "description_column": description_column,  # Best guess (singular, for config template)
                    "has_nested_code_info": has_code_info,
                    "has_nested_charges": has_standard_charges,
                    "has_drug_info": has_drug_info,
                    "record_keys": list(all_keys)[:20]
                }
                
                # Analyze nested structure if present
                if has_code_info and isinstance(first_record.get('code_information'), list):
                    code_info = first_record['code_information']
                    if code_info and isinstance(code_info[0], dict):
                        profile["detected_patterns"]["code_info_keys"] = list(code_info[0].keys())
                
                if has_standard_charges and isinstance(first_record.get('standard_charges'), list):
                    charges = first_record['standard_charges']
                    if charges and isinstance(charges[0], dict):
                        profile["detected_patterns"]["charge_keys"] = list(charges[0].keys())
                
                # Generate config template for JSON files
                profile["config_template"] = generate_config_template(profile, hospital_name=None)
            
    except json.JSONDecodeError as e:
        profile["errors"].append(f"JSON parse error: {str(e)}")
    except MemoryError:
        profile["errors"].append("File too large to parse in memory")
    except Exception as e:
        profile["errors"].append(str(e))
    
    return profile


def process_hospital_worker(args):
    """
    Worker function for parallel processing.
    Takes (hospital_id, download_info) tuple and returns (hospital_id, status, manifest_update).
    """
    hospital_id, download_info = args
    return process_hospital(hospital_id, download_info)


def process_hospital(hospital_id, download_info):
    """
    Process a single hospital's downloaded file.
    Returns (status, manifest_update_dict) where manifest_update_dict contains the entry to add to analysis_manifest["analyses"].
    """
    hospital_name = download_info.get("name", "Unknown")
    file_path = download_info.get("file_path")
    file_type = download_info.get("file_type", "csv")
    
    # Note: We don't check if already analyzed here - that's done in main() before submitting to workers
    
    print(f"\n{'='*60}")
    print(f"Analyzing: {hospital_name}")
    print(f"File: {file_path}")
    print(f"Type: {file_type}")
    
    if not file_path or not Path(file_path).exists():
        print("  ⚠️  File not found")
        manifest_update = {
            "name": hospital_name,
            "status": "file_not_found",
            "timestamp": datetime.now().isoformat()
        }
        return ("failed", manifest_update)
    
    file_path = Path(file_path)
    actual_file_type = file_type
    
    # Handle ZIP files - extract and find data file
    if file_type == "zip":
        print("  📦 Extracting ZIP file...")
        extract_dir, data_files = extract_zip_file(file_path)
        
        if not data_files:
            print("  ⚠️  No data files found in ZIP")
            manifest_update = {
                "name": hospital_name,
                "status": "empty_zip",
                "file_type": file_type,
                "timestamp": datetime.now().isoformat()
            }
            return ("failed", manifest_update)
        
        # Use the first (or largest) data file
        data_files.sort(key=lambda x: x.stat().st_size, reverse=True)
        file_path = data_files[0]
        actual_file_type = file_path.suffix.lower().strip('.')
        print(f"  📁 Found: {file_path.name} ({actual_file_type})")
    
    # Analyze based on file type
    try:
        if actual_file_type in ["csv", "xlsx", "xls"]:
            file_type_label = "Excel" if actual_file_type in ["xlsx", "xls"] else "CSV"
            print(f"  📊 Analyzing {file_type_label} structure...")
            profile = analyze_csv_file(file_path)  # Handles both CSV and Excel
            # Add hospital name to profile for config template generation
            profile["hospital_name"] = hospital_name
        elif actual_file_type == "json":
            print("  📋 Analyzing JSON structure...")
            profile = analyze_json_file(file_path)
            # Add hospital name to profile for config template generation
            profile["hospital_name"] = hospital_name
        else:
            print(f"  ⚠️  Unsupported file type: {actual_file_type}")
            manifest_update = {
                "name": hospital_name,
                "status": "unsupported_type",
                "file_type": actual_file_type,
                "timestamp": datetime.now().isoformat()
            }
            return ("failed", manifest_update)
        
        # Save profile with sanitized hospital name (matching downloads folder)
        safe_name = sanitize_filename(hospital_name)
        profile_file = PROFILES_DIR / f"{safe_name}.json"
        with open(profile_file, 'w') as f:
            json.dump(profile, f, indent=2, default=str)
        
        # Get row/record count safely
        total_rows = profile.get("total_rows") or profile.get("total_records") or 0
        total_cols = profile.get("total_columns") or 0
        
        # Create manifest update
        manifest_update = {
            "name": hospital_name,
            "sanitized_name": safe_name,  # Store for file lookup
            "status": "completed",
            "profile_file": str(profile_file),
            "file_type": actual_file_type,
            "original_type": file_type,
            "format_type": profile.get("format_type"),
            "total_rows": total_rows,
            "total_columns": total_cols,
            "warnings": len(profile.get("warnings", [])),
            "errors": len(profile.get("errors", [])),
            "timestamp": datetime.now().isoformat()
        }
        
        # Print summary with safe formatting
        print(f"  ✅ Analysis complete!")
        print(f"     Format: {profile.get('format_type', 'unknown')}")
        print(f"     Rows/Records: {format_number(total_rows)}")
        print(f"     Columns/Keys: {format_number(total_cols)}")
        
        if profile.get("detected_patterns"):
            patterns = profile["detected_patterns"]
            if patterns.get("code_columns"):
                print(f"     Code columns: {', '.join(str(c) for c in patterns['code_columns'][:3])}")
            if patterns.get("price_columns"):
                print(f"     Price columns: {len(patterns['price_columns'])} found")
            if patterns.get("has_nested_charges"):
                print(f"     Has nested charge structure: Yes")
            if patterns.get("record_keys"):
                print(f"     Record keys: {len(patterns['record_keys'])} found")
        
        if profile.get("warnings"):
            print(f"     ⚠️  Warnings: {len(profile['warnings'])}")
        if profile.get("errors"):
            print(f"     ❌ Errors: {len(profile['errors'])}")
        
        return ("completed", manifest_update)
        
    except Exception as e:
        print(f"  ❌ Analysis failed: {str(e)}")
        manifest_update = {
            "name": hospital_name,
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
        return ("failed", manifest_update)


def main():
    """Main analysis orchestrator."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze CSV/JSON/Excel files and generate profiles")
    parser.add_argument('--hospital-id', type=str, help='Analyze a specific hospital by ID')
    args = parser.parse_args()
    
    print("=" * 60)
    print("  HOSPITAL FILE ANALYZER - Phase 2")
    print("=" * 60)
    
    # Load download manifest
    download_manifest = load_manifest()
    
    # Get completed downloads
    completed_downloads = {
        k: v for k, v in download_manifest.get("downloads", {}).items()
        if v.get("status") == "completed"
    }
    
    # Filter to specific hospital if requested
    if args.hospital_id:
        if args.hospital_id not in completed_downloads:
            print(f"ERROR: Hospital ID '{args.hospital_id}' not found in download manifest")
            print(f"Available hospitals: {list(completed_downloads.keys())[:5]}...")
            sys.exit(1)
        completed_downloads = {args.hospital_id: completed_downloads[args.hospital_id]}
        print(f"Processing single hospital: {completed_downloads[args.hospital_id].get('name', args.hospital_id)}")
    
    print(f"Found {len(completed_downloads)} completed downloads to analyze")
    
    # Create profiles directory
    PROFILES_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load/create analysis manifest
    analysis_manifest = load_analysis_manifest()
    analysis_manifest["stats"]["total"] = len(completed_downloads)
    
    # Count existing analyses
    already_done = sum(1 for h_id in completed_downloads 
                       if analysis_manifest["analyses"].get(h_id, {}).get("status") == "completed")
    print(f"Already analyzed: {already_done}")
    print(f"Remaining: {len(completed_downloads) - already_done}")
    
    # Filter out already completed hospitals
    hospitals_to_process = []
    hospital_info_map = {}  # Map hospital_id -> download_info for error handling
    for hospital_id, download_info in completed_downloads.items():
        hospital_info_map[hospital_id] = download_info
        if hospital_id not in analysis_manifest["analyses"]:
            hospitals_to_process.append((hospital_id, download_info))
        else:
            status = analysis_manifest["analyses"][hospital_id].get("status")
            if status == "completed":
                # Already done, skip
                continue
            else:
                # Retry failed/skipped hospitals
                hospitals_to_process.append((hospital_id, download_info))
    
    if not hospitals_to_process:
        print("\n✅ All hospitals already analyzed!")
        return
    
    # Process hospitals in parallel
    stats = {"completed": 0, "failed": 0, "skipped": 0}
    total_to_process = len(hospitals_to_process)
    skipped_count = len(completed_downloads) - total_to_process
    
    print(f"\nProcessing {total_to_process} hospitals...")
    num_workers = min(8, multiprocessing.cpu_count())  # Use 8 workers or CPU count, whichever is less
    print(f"Using parallel processing with {num_workers} workers...")
    
    try:
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            # Submit all tasks
            future_to_hospital = {
                executor.submit(process_hospital_worker, (hospital_id, download_info)): hospital_id
                for hospital_id, download_info in hospitals_to_process
            }
            
            # Process completed tasks as they finish
            completed_count = 0
            for future in as_completed(future_to_hospital):
                hospital_id = future_to_hospital[future]
                completed_count += 1
                
                try:
                    status, manifest_update = future.result()
                    stats[status] = stats.get(status, 0) + 1
                    
                    # Update manifest with result
                    analysis_manifest["analyses"][hospital_id] = manifest_update
                    
                    # Progress update every 5 completions
                    if completed_count % 5 == 0 or completed_count == total_to_process:
                        print(f"  [{completed_count}/{total_to_process}] Completed processing...")
                        # Save manifest periodically
                        analysis_manifest["stats"] = stats
                        save_analysis_manifest(analysis_manifest)
                        
                except Exception as e:
                    print(f"  ⚠️  Error processing {hospital_id}: {e}")
                    stats["failed"] = stats.get("failed", 0) + 1
                    # Add error entry to manifest
                    download_info = hospital_info_map.get(hospital_id, {})
                    analysis_manifest["analyses"][hospital_id] = {
                        "name": download_info.get("name", "Unknown"),
                        "status": "failed",
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    }
                
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Progress saved.")
    
    # Final save
    analysis_manifest["stats"] = stats
    save_analysis_manifest(analysis_manifest)
    
    # Summary
    print("\n" + "=" * 60)
    print("  ANALYSIS SUMMARY")
    print("=" * 60)
    print(f"  Total Files: {len(completed_downloads)}")
    print(f"  ✅ Completed: {stats.get('completed', 0)}")
    print(f"  ⏭️  Skipped (already done): {skipped_count}")
    print(f"  ❌ Failed: {stats.get('failed', 0)}")
    print(f"\n  Profiles saved to: {PROFILES_DIR}")
    print(f"  Manifest saved to: {ANALYSIS_MANIFEST}")
    print("=" * 60)


if __name__ == "__main__":
    main()

