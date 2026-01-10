#!/usr/bin/env python3
"""
Shared Data Extraction Module

Common extraction functions used by preview_cards.py and bulk_ingest.py.
Single source of truth for all data extraction logic.
"""

import json
import pandas as pd


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
    Handles pandas Series by converting to scalar first.
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
    
    Returns (code, code_type) or (None, None)
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


def extract_setting(row, config, is_json=False):
    """
    Extract the setting (inpatient/outpatient) from a row.
    Handles both CSV (pandas Series) and JSON (dict) formats.
    """
    setting_ext = config.get('setting_extraction', {})
    
    primary = setting_ext.get('primary', 'setting')
    fallback = setting_ext.get('fallback', 'billing_class')
    default = setting_ext.get('default', 'UNKNOWN')
    
    # Handle JSON format - setting might be nested in standard_charges
    if is_json:
        # First try top-level
        if primary:
            val = safe_get_value(row, primary)
            if val is not None and str(val).strip() and str(val) != 'nan':
                return str(val).strip()
        
        # If not found, try inside standard_charges
        if 'standard_charges' in (row.keys() if isinstance(row, dict) else row.index if isinstance(row, pd.Series) else []):
            sc_val = safe_get_value(row, 'standard_charges')
            sc_parsed = parse_json_value(sc_val)
            
            if isinstance(sc_parsed, list) and len(sc_parsed) > 0:
                charge_obj = sc_parsed[0]
                if isinstance(charge_obj, dict):
                    # Try primary, then fallback
                    if primary:
                        val = charge_obj.get(primary)
                        if val is not None and str(val).strip():
                            return str(val).strip()
                    if fallback:
                        val = charge_obj.get(fallback)
                        if val is not None and str(val).strip():
                            return str(val).strip()
            elif isinstance(sc_parsed, dict):
                # Try primary, then fallback
                if primary:
                    val = sc_parsed.get(primary)
                    if val is not None and str(val).strip():
                        return str(val).strip()
                if fallback:
                    val = sc_parsed.get(fallback)
                    if val is not None and str(val).strip():
                        return str(val).strip()
    else:
        # CSV format - direct column lookup
        if primary:
            val = safe_get_value(row, primary)
            if val is not None:
                # Ensure val is a scalar (not a Series) before boolean checks
                if isinstance(val, pd.Series):
                    val = val.iloc[0] if len(val) > 0 else None
                if val is not None and str(val).strip() and str(val) != 'nan':
                    return str(val).strip()
        
        # Try fallback
        if fallback:
            val = safe_get_value(row, fallback)
            if val is not None:
                # Ensure val is a scalar (not a Series) before boolean checks
                if isinstance(val, pd.Series):
                    val = val.iloc[0] if len(val) > 0 else None
                if val is not None and str(val).strip() and str(val) != 'nan':
                    return str(val).strip()
    
    return default


def extract_code(row, config, is_json=False):
    """
    Extract the best code from a row using the config's code_extraction rules.
    Returns (code, code_type).
    Handles both CSV (pandas Series) and JSON (dict) formats.
    """
    code_ext = config.get('code_extraction', {})
    columns = code_ext.get('columns', [])
    type_columns = code_ext.get('type_columns', [])
    priority = code_ext.get('priority', ['CPT', 'HCPCS', 'MS-DRG', 'APR-DRG', 'NDC', 'CDM', 'Local'])
    auto_normalize = code_ext.get('auto_normalize', True)
    
    # Build priority map (lower = better)
    priority_map = {code_type: i for i, code_type in enumerate(priority)}
    priority_map['UNKNOWN'] = 999
    
    # Get available column names
    if isinstance(row, pd.Series):
        available_cols = set(row.index)
    elif isinstance(row, dict):
        available_cols = set(row.keys())
    else:
        available_cols = set()
    
    # Fallback: if no code_extraction, use old-style single column
    if not columns:
        code_col = config.get('code_column', 'code|1')
        type_col = config.get('code_type_column')
        
        # For JSON files: Handle CSV-style column names (e.g., "code_information|1" -> "code_information")
        actual_code_col = code_col
        if is_json and '|' in str(code_col):
            actual_code_col = str(code_col).split('|')[0]
            if actual_code_col not in available_cols:
                actual_code_col = code_col  # Fallback to original
        
        code_val = safe_get_value(row, actual_code_col)
        
        # For JSON, code might be nested
        if is_json and code_val is not None:
            code, code_type = extract_code_from_value(code_val)
            if code:
                return code, code_type or 'UNKNOWN'
        
        code = code_val if code_val else 'UNKNOWN'
        code_type = safe_get_value(row, type_col, 'UNKNOWN') if type_col else 'UNKNOWN'
        return str(code).strip() if code else 'UNKNOWN', code_type
    
    # Find best code by priority
    best_code = 'UNKNOWN'
    best_type = 'UNKNOWN'
    best_priority = 999
    
    # Auto-detect type columns if not provided (handle incorrectly generated configs)
    # If type_columns is None/empty, check if columns list contains type columns
    auto_type_columns = []
    code_only_columns = []
    if not type_columns:
        for col in columns:
            col_str = str(col)
            # Check if column name suggests it's a type column (ends with |type or contains |type|)
            if '|type' in col_str.lower() or col_str.endswith('|type'):
                auto_type_columns.append(col)
            else:
                code_only_columns.append(col)
        # If we found type columns mixed in, use them
        if auto_type_columns and code_only_columns:
            # Match type columns to code columns by index/name
            # e.g., 'code|1' -> 'code|1|type', 'code|2' -> 'code|2|type'
            type_columns = []
            for code_col in code_only_columns:
                # Try to find matching type column
                code_col_str = str(code_col)
                matching_type = None
                for type_col in auto_type_columns:
                    type_col_str = str(type_col)
                    # Check if type column matches (e.g., 'code|1|type' matches 'code|1')
                    if code_col_str in type_col_str or type_col_str.startswith(code_col_str.split('|')[0]):
                        matching_type = type_col
                        break
                type_columns.append(matching_type)
            columns = code_only_columns  # Use only code columns
        elif not code_only_columns:
            # All columns are type columns? This is wrong, but try to extract codes anyway
            pass
    
    for i, col in enumerate(columns):
        # For JSON files: Handle CSV-style column names
        actual_col = col
        if is_json and '|' in str(col):
            actual_col = str(col).split('|')[0]
            if actual_col not in available_cols:
                actual_col = col  # Fallback to original
        
        if actual_col not in available_cols:
            continue
        
        code_val = safe_get_value(row, actual_col)
        if code_val is None:
            continue
        
        # Skip if this looks like a type column (contains common type values)
        # This handles cases where type columns are incorrectly in the columns list
        if isinstance(code_val, str):
            code_val_upper = code_val.strip().upper()
            # Common code types that shouldn't be treated as codes
            if code_val_upper in ['CPT', 'HCPCS', 'MS-DRG', 'APR-DRG', 'NDC', 'CDM', 'LOCAL', 'RC', 'ICD', 'REVENUE']:
                # This is likely a type column, skip it
                continue
        
        # Try to extract code (handles JSON structures)
        code, code_type = extract_code_from_value(code_val)
        if not code:
            code = str(code_val).strip()
        
        if code and code != 'nan' and code != '':
            # Try to get type from corresponding type column
            if type_columns and i < len(type_columns):
                type_col = type_columns[i]
                if type_col:  # type_col might be None if no match found
                    # Also handle CSV-style type column names for JSON
                    if is_json and '|' in str(type_col):
                        type_col = str(type_col).split('|')[0]
                        if type_col not in available_cols:
                            type_col = type_columns[i]  # Fallback
                    
                    if type_col and type_col in available_cols:
                        type_val = safe_get_value(row, type_col)
                        if type_val is not None:
                            code_type = str(type_val).strip()
            
            # If still no type, try to find a matching type column by name pattern
            if not code_type or code_type == 'UNKNOWN':
                col_str = str(col)
                # Look for a column like 'code|1|type' when we have 'code|1'
                for avail_col in available_cols:
                    avail_col_str = str(avail_col)
                    if '|type' in avail_col_str.lower() and col_str in avail_col_str:
                        type_val = safe_get_value(row, avail_col)
                        if type_val is not None:
                            code_type = str(type_val).strip()
                            break
            
            # Check priority
            code_priority = priority_map.get(code_type or 'UNKNOWN', 999)
            if code_priority < best_priority:
                best_code = code
                best_type = code_type or 'UNKNOWN'
                best_priority = code_priority
    
    return best_code, best_type


class PriceExtractor:
    """
    Unified price extraction for all formats (CSV header, CSV column, JSON).
    """
    
    def __init__(self, config, row, available_cols, is_json=False):
        self.config = config
        self.row = row
        self.available_cols = available_cols
        self.is_json = is_json
        self.price_ext = config.get('price_extraction', {})
        self.payer_style = self.price_ext.get('payer_style', 'column')
    
    def extract_all_prices(self):
        """
        Extract all prices from a row.
        Returns list of dicts with keys: payer, amount, percentage, methodology, notes
        For backward compatibility, dicts are also tuple-like (can be unpacked as payer, amount).
        """
        prices = []
        
        if self.is_json:
            price_list = self._extract_json_prices()
        elif self.payer_style == 'header':
            price_list = self._extract_header_style_prices()
        else:
            price_list = self._extract_column_style_prices()
        
        # Convert to dict format for richer data
        result = []
        for price_info in price_list:
            if isinstance(price_info, dict):
                result.append(price_info)
            elif isinstance(price_info, tuple):
                # Legacy tuple format: (payer, amount)
                payer, amount = price_info
                result.append({
                    'payer': payer,
                    'amount': amount,
                    'percentage': None,
                    'methodology': None,
                    'notes': None
                })
        
        return result
    
    def _extract_json_prices(self):
        """Extract prices from JSON format."""
        prices = []
        
        if 'standard_charges' not in self.available_cols:
            return prices
        
        sc_val = safe_get_value(self.row, 'standard_charges')
        sc_parsed = parse_json_value(sc_val)
        
        if isinstance(sc_parsed, list) and len(sc_parsed) > 0:
            charge_obj = sc_parsed[0]
            if isinstance(charge_obj, dict):
                # Extract GROSS and CASH
                if 'gross_charge' in charge_obj:
                    gross_val = charge_obj['gross_charge']
                    if gross_val is not None:
                        prices.append({
                            'payer': 'GROSS',
                            'amount': str(gross_val).strip(),
                            'percentage': None,
                            'methodology': None,
                            'notes': None
                        })
                
                if 'discounted_cash' in charge_obj:
                    cash_val = charge_obj['discounted_cash']
                    if cash_val is not None:
                        prices.append({
                            'payer': 'CASH',
                            'amount': str(cash_val).strip(),
                            'percentage': None,
                            'methodology': None,
                            'notes': None
                        })
                
                # Extract from payers_information array
                if 'payers_information' in charge_obj:
                    payers_info = charge_obj['payers_information']
                    if isinstance(payers_info, list):
                        for payer_obj in payers_info:
                            if isinstance(payer_obj, dict):
                                payer_name = payer_obj.get('payer_name') or payer_obj.get('payer')
                                plan_name = payer_obj.get('plan_name') or payer_obj.get('plan')
                                estimated = payer_obj.get('estimated_amount') or payer_obj.get('negotiated_dollar')
                                
                                if payer_name:
                                    display_name = str(payer_name).strip()
                                    if plan_name:
                                        display_name = f"{display_name} ({str(plan_name).strip()[:20]})"
                                    
                                    # Check for percentage if no dollar amount
                                    percentage = payer_obj.get('negotiated_percentage') or payer_obj.get('percentage')
                                    methodology = payer_obj.get('methodology') or payer_obj.get('methodology_type')
                                    
                                    if estimated is not None:
                                        prices.append({
                                            'payer': display_name[:40],
                                            'amount': str(estimated).strip(),
                                            'percentage': str(percentage).strip() if percentage else None,
                                            'methodology': str(methodology).strip() if methodology else None,
                                            'notes': None
                                        })
                                    elif percentage is not None:
                                        # Percentage-based pricing (no dollar amount)
                                        prices.append({
                                            'payer': display_name[:40],
                                            'amount': None,
                                            'percentage': str(percentage).strip(),
                                            'methodology': str(methodology).strip() if methodology else None,
                                            'notes': None
                                        })
        
        elif isinstance(sc_parsed, dict):
            # standard_charges is a single object
            charge_obj = sc_parsed
            if 'gross_charge' in charge_obj:
                prices.append({
                    'payer': 'GROSS',
                    'amount': str(charge_obj['gross_charge']).strip(),
                    'percentage': None,
                    'methodology': None,
                    'notes': None
                })
            if 'discounted_cash' in charge_obj:
                prices.append({
                    'payer': 'CASH',
                    'amount': str(charge_obj['discounted_cash']).strip(),
                    'percentage': None,
                    'methodology': None,
                    'notes': None
                })
            
            # Extract from payers_information array
            if 'payers_information' in charge_obj:
                payers_info = charge_obj['payers_information']
                if isinstance(payers_info, list):
                    for payer_obj in payers_info:
                        if isinstance(payer_obj, dict):
                            payer_name = payer_obj.get('payer_name') or payer_obj.get('payer')
                            plan_name = payer_obj.get('plan_name') or payer_obj.get('plan')
                            estimated = payer_obj.get('estimated_amount') or payer_obj.get('negotiated_dollar')
                            
                            if payer_name:
                                display_name = str(payer_name).strip()
                                if plan_name:
                                    display_name = f"{display_name} ({str(plan_name).strip()[:20]})"
                                
                                # Check for percentage if no dollar amount
                                percentage = payer_obj.get('negotiated_percentage') or payer_obj.get('percentage')
                                methodology = payer_obj.get('methodology') or payer_obj.get('methodology_type')
                                
                                if estimated is not None:
                                    prices.append({
                                        'payer': display_name[:40],
                                        'amount': str(estimated).strip(),
                                        'percentage': str(percentage).strip() if percentage else None,
                                        'methodology': str(methodology).strip() if methodology else None,
                                        'notes': None
                                    })
                                elif percentage is not None:
                                    # Percentage-based pricing (no dollar amount)
                                    prices.append({
                                        'payer': display_name[:40],
                                        'amount': None,
                                        'percentage': str(percentage).strip(),
                                        'methodology': str(methodology).strip() if methodology else None,
                                        'notes': None
                                    })
        
        return prices
    
    def _extract_header_style_prices(self):
        """Extract prices from CSV header-style format (payers in column names)."""
        prices = []
        
        # Always try GROSS and CASH first
        gross_col = self.price_ext.get('gross_column', 'standard_charge|gross')
        cash_col = self.price_ext.get('cash_column', 'standard_charge|discounted_cash')
        
        if gross_col and gross_col in self.available_cols:
            gross_val = safe_get_value(self.row, gross_col)
            if gross_val is not None and str(gross_val).strip() and str(gross_val) != 'nan':
                prices.append({
                    'payer': 'GROSS',
                    'amount': str(gross_val).strip(),
                    'percentage': None,
                    'methodology': None,
                    'notes': None
                })
        
        if cash_col and cash_col in self.available_cols:
            cash_val = safe_get_value(self.row, cash_col)
            if cash_val is not None and str(cash_val).strip() and str(cash_val) != 'nan':
                prices.append({
                    'payer': 'CASH',
                    'amount': str(cash_val).strip(),
                    'percentage': None,
                    'methodology': None,
                    'notes': None
                })
        
        # Scan for all negotiated_dollar or estimated_amount columns
        for col in self.available_cols:
            col_str = str(col)
            if 'negotiated_dollar' in col_str or 'estimated_amount' in col_str:
                price_val = safe_get_value(self.row, col)
                if price_val is not None and str(price_val).strip() and str(price_val) != 'nan':
                    # Parse payer from column name
                    parts = col_str.split('|')
                    payer = parts[1] if len(parts) > 1 else 'Unknown'
                    # Only add if it's a numeric value (not percentage)
                    try:
                        float(str(price_val).replace('$', '').replace(',', ''))
                        prices.append({
                            'payer': payer[:20],
                            'amount': str(price_val).strip(),
                            'percentage': None,
                            'methodology': None,
                            'notes': None
                        })
                    except:
                        pass  # Skip non-numeric (like percentages)
        
        return prices
    
    def _extract_column_style_prices(self):
        """Extract prices from CSV column-style format (payer in separate column)."""
        prices = []
        
        price_col = self.price_ext.get('price_column', 'standard_charge|negotiated_dollar')
        payer_col = self.price_ext.get('payer_column', 'payer_name')
        percentage_col = self.price_ext.get('percentage_column', 'standard_charge|negotiated_percentage')
        methodology_col = self.price_ext.get('methodology_column', 'standard_charge|methodology')
        
        payer = safe_get_value(self.row, payer_col, 'Unknown') if payer_col and payer_col in self.available_cols else 'Unknown'
        
        # Try to extract dollar amount first
        amount = None
        if price_col and price_col in self.available_cols:
            price_val = safe_get_value(self.row, price_col)
            if price_val is not None and str(price_val).strip() and str(price_val) != 'nan':
                amount = str(price_val).strip()
        
        # If no dollar amount, try to extract percentage
        percentage = None
        methodology = None
        if not amount or amount == '':
            # Check for percentage column
            if percentage_col and percentage_col in self.available_cols:
                pct_val = safe_get_value(self.row, percentage_col)
                if pct_val is not None and str(pct_val).strip() and str(pct_val) != 'nan':
                    percentage = str(pct_val).strip()
            
            # Check for methodology column
            if methodology_col and methodology_col in self.available_cols:
                meth_val = safe_get_value(self.row, methodology_col)
                if meth_val is not None and str(meth_val).strip() and str(meth_val) != 'nan':
                    methodology = str(meth_val).strip()
        
        # Only add if we have either amount or percentage
        if amount or percentage:
            prices.append({
                'payer': str(payer)[:20],
                'amount': amount,
                'percentage': percentage,
                'methodology': methodology,
                'notes': None
            })
        
        return prices
