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

# Configuration
DATA_DIR = Path(__file__).parent.parent.parent / "data"
DOWNLOADS_DIR = DATA_DIR / "downloads"
PROFILES_DIR = DATA_DIR / "profiles"
MANIFEST_FILE = DOWNLOADS_DIR / "download_manifest.json"
ANALYSIS_MANIFEST = PROFILES_DIR / "analysis_manifest.json"

# Sample size for very large files (rows to analyze)
MAX_ROWS_TO_ANALYZE = 50000  # Analyze up to 50K rows
SAMPLE_SIZE = 10  # Number of sample values to store
MAX_JSON_RECORDS = 10000  # Analyze up to 10K records for JSON


def format_number(value, default="N/A"):
    """Safely format a number with commas, handling non-numeric values."""
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return f"{value:,}"
    return str(value)


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
    """Detect file encoding by trying common encodings."""
    encodings = ['utf-8', 'utf-8-sig', 'iso-8859-1', 'cp1252', 'latin1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(10000)  # Try reading first 10KB
            return encoding
        except (UnicodeDecodeError, UnicodeError):
            continue
    
    return 'utf-8'  # Default fallback


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
    col_lower = column_name.lower()
    inferred_purpose = "unknown"
    
    if any(k in col_lower for k in ['code', 'cpt', 'hcpcs', 'icd', 'drg', 'ndc']):
        inferred_purpose = "code"
    elif any(k in col_lower for k in ['desc', 'name', 'procedure', 'service']):
        inferred_purpose = "description"
    elif any(k in col_lower for k in ['charge', 'price', 'amount', 'rate', 'dollar', 'cost']):
        inferred_purpose = "price"
    elif any(k in col_lower for k in ['payer', 'insurance', 'carrier']):
        inferred_purpose = "payer"
    elif any(k in col_lower for k in ['plan', 'product']):
        inferred_purpose = "plan"
    elif any(k in col_lower for k in ['type', 'class', 'category', 'setting']):
        inferred_purpose = "category"
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


def analyze_csv_file(file_path):
    """
    Analyze a CSV file and return a comprehensive profile.
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
        # Detect encoding
        encoding = detect_encoding(file_path)
        profile["encoding"] = encoding
        
        # Read file
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            # First, read some rows to detect header
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
            
            columns = first_rows[header_row_idx]
            profile["columns"] = columns
            profile["total_columns"] = len(columns)
            
            # Reset file and skip to data
            f.seek(0)
            reader = csv.reader(f)
            
            # Skip to header
            for _ in range(header_row_idx + 1):
                next(reader, None)
            
            # Read data rows (up to MAX_ROWS_TO_ANALYZE)
            data_rows = []
            row_count = 0
            
            for row in reader:
                if row_count >= MAX_ROWS_TO_ANALYZE:
                    break
                data_rows.append(row)
                row_count += 1
            
            profile["total_rows"] = row_count
            
            if row_count >= MAX_ROWS_TO_ANALYZE:
                profile["warnings"].append(f"File truncated for analysis (analyzed {MAX_ROWS_TO_ANALYZE} rows)")
        
        # Analyze each column
        for col_idx, col_name in enumerate(columns):
            col_values = []
            for row in data_rows:
                if col_idx < len(row):
                    col_values.append(row[col_idx])
                else:
                    col_values.append(None)
            
            col_analysis = analyze_column(col_values, col_name)
            profile["column_analyses"].append(col_analysis)
        
        # Detect format type
        profile["format_type"] = detect_format_type(columns, profile["column_analyses"])
        
        # Identify key columns
        code_columns = [a["column_name"] for a in profile["column_analyses"] 
                       if a["inferred_purpose"] == "code" or a["likely_type"] == "code"]
        desc_columns = [a["column_name"] for a in profile["column_analyses"] 
                       if a["inferred_purpose"] == "description"]
        price_columns = [a["column_name"] for a in profile["column_analyses"] 
                        if a["inferred_purpose"] in ["price", "standard_charge", "negotiated_rate"] 
                        or a["likely_type"] == "price"]
        
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
        
        profile["detected_patterns"] = {
            "code_columns": code_columns[:5],  # Top 5
            "description_columns": desc_columns[:3],
            "price_columns": price_columns[:20],  # Can have many in wide format
            "has_payer_column": any(a["inferred_purpose"] == "payer" for a in profile["column_analyses"]),
            "has_plan_column": any(a["inferred_purpose"] == "plan" for a in profile["column_analyses"]),
            "has_notes_column": any(a["inferred_purpose"] == "notes" for a in profile["column_analyses"]),
            "has_header_style_payers": has_header_style_payers,
            "header_style_payer_columns": header_style_payer_columns[:10] if has_header_style_payers else []  # Store examples
        }
        
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
        
        # Analyze the records structure
        if records and len(records) > 0:
            # Sample records for analysis (up to MAX_JSON_RECORDS)
            sample_count = min(len(records), MAX_JSON_RECORDS)
            sample_records = records[:sample_count]
            
            if sample_count < len(records):
                profile["warnings"].append(f"Analyzed {sample_count:,} of {len(records):,} records")
            
            # Store sample record
            first_record = records[0]
            if len(str(first_record)) < 5000:
                profile["sample_record"] = first_record
            else:
                profile["sample_record"] = {"note": "Record too large, showing keys only", "keys": list(first_record.keys()) if isinstance(first_record, dict) else []}
            
            # Analyze record structure
            if isinstance(first_record, dict):
                # Get all unique keys across sample records
                all_keys = set()
                for record in sample_records[:1000]:  # Check first 1000 for keys
                    if isinstance(record, dict):
                        all_keys.update(record.keys())
                
                profile["columns"] = list(all_keys)
                profile["total_columns"] = len(all_keys)
                
                # Analyze each "column" (key)
                for key in all_keys:
                    values = []
                    for record in sample_records:
                        if isinstance(record, dict):
                            val = record.get(key)
                            # Flatten nested structures for analysis
                            if isinstance(val, (list, dict)):
                                values.append(str(val)[:100])  # Truncate complex values
                            else:
                                values.append(val)
                    
                    col_analysis = analyze_column(values, key)
                    profile["column_analyses"].append(col_analysis)
                
                # Detect patterns specific to JSON hospital files
                code_cols = [a["column_name"] for a in profile["column_analyses"] 
                            if a["inferred_purpose"] == "code" or 'code' in a["column_name"].lower()]
                desc_cols = [a["column_name"] for a in profile["column_analyses"] 
                            if a["inferred_purpose"] == "description" or 'desc' in a["column_name"].lower()]
                
                # Check for nested structures common in hospital JSON
                has_code_info = 'code_information' in all_keys
                has_standard_charges = 'standard_charges' in all_keys
                has_drug_info = 'drug_information' in all_keys
                
                profile["detected_patterns"] = {
                    "code_columns": code_cols[:5],
                    "description_columns": desc_cols[:3],
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
            
    except json.JSONDecodeError as e:
        profile["errors"].append(f"JSON parse error: {str(e)}")
    except MemoryError:
        profile["errors"].append("File too large to parse in memory")
    except Exception as e:
        profile["errors"].append(str(e))
    
    return profile


def process_hospital(hospital_id, download_info, analysis_manifest):
    """
    Process a single hospital's downloaded file.
    Returns status string.
    """
    hospital_name = download_info.get("name", "Unknown")
    file_path = download_info.get("file_path")
    file_type = download_info.get("file_type", "csv")
    
    # Check if already analyzed
    if hospital_id in analysis_manifest["analyses"]:
        status = analysis_manifest["analyses"][hospital_id].get("status")
        if status == "completed":
            return "skipped"
    
    print(f"\n{'='*60}")
    print(f"Analyzing: {hospital_name}")
    print(f"File: {file_path}")
    print(f"Type: {file_type}")
    
    if not file_path or not Path(file_path).exists():
        print("  ⚠️  File not found")
        analysis_manifest["analyses"][hospital_id] = {
            "name": hospital_name,
            "status": "file_not_found",
            "timestamp": datetime.now().isoformat()
        }
        return "failed"
    
    file_path = Path(file_path)
    actual_file_type = file_type
    
    # Handle ZIP files - extract and find data file
    if file_type == "zip":
        print("  📦 Extracting ZIP file...")
        extract_dir, data_files = extract_zip_file(file_path)
        
        if not data_files:
            print("  ⚠️  No data files found in ZIP")
            analysis_manifest["analyses"][hospital_id] = {
                "name": hospital_name,
                "status": "empty_zip",
                "file_type": file_type,
                "timestamp": datetime.now().isoformat()
            }
            return "failed"
        
        # Use the first (or largest) data file
        data_files.sort(key=lambda x: x.stat().st_size, reverse=True)
        file_path = data_files[0]
        actual_file_type = file_path.suffix.lower().strip('.')
        print(f"  📁 Found: {file_path.name} ({actual_file_type})")
    
    # Analyze based on file type
    try:
        if actual_file_type in ["csv", "xlsx", "xls"]:
            print("  📊 Analyzing CSV structure...")
            profile = analyze_csv_file(file_path)
        elif actual_file_type == "json":
            print("  📋 Analyzing JSON structure...")
            profile = analyze_json_file(file_path)
        else:
            print(f"  ⚠️  Unsupported file type: {actual_file_type}")
            analysis_manifest["analyses"][hospital_id] = {
                "name": hospital_name,
                "status": "unsupported_type",
                "file_type": actual_file_type,
                "timestamp": datetime.now().isoformat()
            }
            return "failed"
        
        # Save profile
        profile_file = PROFILES_DIR / f"{hospital_id}.json"
        with open(profile_file, 'w') as f:
            json.dump(profile, f, indent=2, default=str)
        
        # Get row/record count safely
        total_rows = profile.get("total_rows") or profile.get("total_records") or 0
        total_cols = profile.get("total_columns") or 0
        
        # Update manifest
        analysis_manifest["analyses"][hospital_id] = {
            "name": hospital_name,
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
        
        return "completed"
        
    except Exception as e:
        print(f"  ❌ Analysis failed: {str(e)}")
        analysis_manifest["analyses"][hospital_id] = {
            "name": hospital_name,
            "status": "failed",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
        return "failed"


def main():
    """Main analysis orchestrator."""
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
    
    # Process each hospital
    stats = {"completed": 0, "failed": 0, "skipped": 0}
    
    try:
        for i, (hospital_id, download_info) in enumerate(completed_downloads.items()):
            print(f"\n[{i+1}/{len(completed_downloads)}]", end="")
            
            status = process_hospital(hospital_id, download_info, analysis_manifest)
            stats[status] = stats.get(status, 0) + 1
            
            # Save manifest periodically
            if (i + 1) % 10 == 0:
                analysis_manifest["stats"] = stats
                save_analysis_manifest(analysis_manifest)
                
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
    print(f"  ⏭️  Skipped (already done): {stats.get('skipped', 0)}")
    print(f"  ❌ Failed: {stats.get('failed', 0)}")
    print(f"\n  Profiles saved to: {PROFILES_DIR}")
    print(f"  Manifest saved to: {ANALYSIS_MANIFEST}")
    print("=" * 60)


if __name__ == "__main__":
    main()

