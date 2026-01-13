#!/usr/bin/env python3
"""
Phase 3: AI Config Generator

Uses Llama 3 via Ollama to generate ingestion configs for each hospital.
Reads profiles from Phase 2 and outputs structured configs for Phase 5.

Features:
- Resumable (tracks progress)
- JSON mode for reliable parsing
- Validates AI output
- Handles tall and wide formats
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
import time

try:
    import ollama
except ImportError:
    print("ERROR: ollama package not installed. Run: pip install ollama")
    sys.exit(1)

# Configuration
DATA_DIR = Path(__file__).parent.parent.parent / "data"
PROFILES_DIR = DATA_DIR / "profiles"
CONFIGS_DIR = DATA_DIR / "configs"
ANALYSIS_MANIFEST = PROFILES_DIR / "analysis_manifest.json"
CONFIG_MANIFEST = CONFIGS_DIR / "config_manifest.json"

# Ollama settings
MODEL_NAME = "mistral:7b-instruct"  # Using mistral:7b-instruct (better instruction following for structured JSON)
TIMEOUT = 120  # seconds per request

# Delay between requests (helps with GPU cooling and game compatibility)
DELAY_BETWEEN_REQUESTS = 2  # seconds


def load_analysis_manifest():
    """Load the analysis manifest from Phase 2."""
    if not ANALYSIS_MANIFEST.exists():
        print(f"ERROR: Analysis manifest not found at {ANALYSIS_MANIFEST}")
        print("Please run analyze_csv.py (Phase 2) first.")
        sys.exit(1)
    
    with open(ANALYSIS_MANIFEST, 'r') as f:
        return json.load(f)


def load_config_manifest():
    """Load or create config manifest."""
    if CONFIG_MANIFEST.exists():
        with open(CONFIG_MANIFEST, 'r') as f:
            return json.load(f)
    return {
        "created": datetime.now().isoformat(),
        "last_updated": None,
        "configs": {},
        "stats": {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0
        }
    }


def save_config_manifest(manifest):
    """Save config manifest."""
    manifest["last_updated"] = datetime.now().isoformat()
    with open(CONFIG_MANIFEST, 'w') as f:
        json.dump(manifest, f, indent=2)


def sanitize_filename(name):
    """Create a safe directory/file name (matches download_all.py)."""
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
    return safe[:100]  # Limit length


def load_profile(hospital_id):
    """Load a hospital's analysis profile."""
    # First try to get sanitized name from analysis manifest
    analysis_manifest = load_analysis_manifest()
    analysis_info = analysis_manifest.get("analyses", {}).get(hospital_id, {})
    sanitized_name = analysis_info.get("sanitized_name")
    
    if sanitized_name:
        profile_file = PROFILES_DIR / f"{sanitized_name}.json"
    else:
        # Fallback: try hospital_id (for backward compatibility)
        profile_file = PROFILES_DIR / f"{hospital_id}.json"
    
    if not profile_file.exists():
        return None
    
    with open(profile_file, 'r') as f:
        return json.load(f)


def validate_and_fix_template_for_json(config_template, format_type):
    """
    Pre-validate and fix config template for JSON files before sending to AI.
    Removes any CSV-style column names (with | separators) from code_extraction.columns.
    Returns (fixed_template, warnings)
    """
    warnings = []
    if format_type != 'json':
        return config_template, warnings
    
    code_ext = config_template.get('code_extraction', {})
    if not code_ext:
        return config_template, warnings
    
    columns = code_ext.get('columns', [])
    if not columns:
        return config_template, warnings
    
    # Check for CSV-style column names
    fixed_columns = []
    for col in columns:
        col_str = str(col)
        if '|' in col_str:
            # Remove everything after | to get the base key name
            base_key = col_str.split('|')[0]
            fixed_columns.append(base_key)
            warnings.append(f"Fixed template: '{col_str}' -> '{base_key}' (removed CSV-style separator)")
        else:
            fixed_columns.append(col)
    
    if warnings:
        # Update the template
        config_template = config_template.copy()
        config_template['code_extraction'] = code_ext.copy()
        config_template['code_extraction']['columns'] = fixed_columns
    
    return config_template, warnings


def auto_fix_json_column_names(config, format_type):
    """
    Post-process AI response to auto-fix CSV-style column names in JSON configs.
    Returns (fixed_config, fixes_applied)
    """
    fixes_applied = []
    if format_type != 'json':
        return config, fixes_applied
    
    code_ext = config.get('code_extraction', {})
    if not code_ext:
        return config, fixes_applied
    
    columns = code_ext.get('columns', [])
    if not columns:
        return config, fixes_applied
    
    # Fix CSV-style column names
    fixed_columns = []
    for col in columns:
        col_str = str(col)
        if '|' in col_str:
            # Remove everything after | to get the base key name
            base_key = col_str.split('|')[0]
            fixed_columns.append(base_key)
            fixes_applied.append(f"Auto-fixed: '{col_str}' -> '{base_key}'")
        else:
            fixed_columns.append(col)
    
    if fixes_applied:
        # Update the config
        config['code_extraction'] = code_ext.copy()
        config['code_extraction']['columns'] = fixed_columns
    
    return config, fixes_applied


def auto_fix_type_columns(config):
    """
    Post-process AI response to auto-fix type columns that are incorrectly placed
    in the columns array instead of type_columns array.
    
    Works for ALL format types (CSV and JSON).
    Detects type columns by patterns: '|type', ends with '|type', ends with '_type'
    
    Returns (fixed_config, fixes_applied)
    """
    fixes_applied = []
    
    code_ext = config.get('code_extraction', {})
    if not code_ext:
        return config, fixes_applied
    
    columns = code_ext.get('columns', [])
    if not columns:
        return config, fixes_applied
    
    # Detect type columns in columns array
    code_only_columns = []
    type_columns_found = []
    
    for col in columns:
        col_str = str(col)
        # Check if this looks like a type column
        is_type_column = (
            '|type' in col_str.lower() or 
            col_str.endswith('|type') or 
            col_str.endswith('_type')
        )
        
        if is_type_column:
            type_columns_found.append(col)
            fixes_applied.append(f"Moved type column '{col_str}' from columns to type_columns")
        else:
            code_only_columns.append(col)
    
    if fixes_applied:
        # Update the config
        config['code_extraction'] = code_ext.copy()
        config['code_extraction']['columns'] = code_only_columns
        
        # Add type columns to existing type_columns array (if any)
        # Handle None explicitly (get() only uses default if key is missing, not if value is None)
        existing_type_columns = code_ext.get('type_columns') or []
        # Merge and deduplicate
        all_type_columns = list(set(existing_type_columns + type_columns_found))
        config['code_extraction']['type_columns'] = all_type_columns
    
    return config, fixes_applied


def create_prompt(profile, hospital_name):
    """
    Create a prompt for AI to complete the config template.
    The template already has all deterministic values from Phase 2.
    AI only needs to fill in semantic mappings and refine column selections.
    """
    # Get config template from profile (generated by Phase 2)
    config_template = profile.get('config_template', {})
    
    # Summarize the profile for the AI
    format_type = profile.get("format_type") or "unknown"  # Handle None explicitly
    columns = profile.get("columns", [])
    column_analyses = profile.get("column_analyses", [])
    detected_patterns = profile.get("detected_patterns", {})
    total_rows = profile.get("total_rows") or profile.get("total_records") or 0
    sample_record = profile.get("sample_record")
    
    # Pre-validate and fix template for JSON files
    config_template, template_warnings = validate_and_fix_template_for_json(config_template, format_type)
    if template_warnings:
        print(f"  ⚠️  Template warnings: {', '.join(template_warnings)}")
    
    # Build column summary
    column_summary = []
    for col in column_analyses[:40]:  # Increased limit for better context
        col_info = f"- {col['column_name']}: {col['likely_type']} ({col['inferred_purpose']}), {col['fill_rate']*100:.0f}% filled"
        if col.get('sample_values'):
            samples = ', '.join(str(v)[:30] for v in col['sample_values'][:3])
            col_info += f", samples: [{samples}]"
        column_summary.append(col_info)
    
    column_text = "\n".join(column_summary) if column_summary else "No column analysis available"
    
    # Build JSON structure explanation if this is a JSON file
    json_structure_text = ""
    if format_type == 'json' and sample_record:
        json_structure_text = "\n"
        json_structure_text += "═══════════════════════════════════════════════════════════════════════════════\n"
        json_structure_text += "🚨 CRITICAL: JSON FILE STRUCTURE - READ THIS FIRST! 🚨\n"
        json_structure_text += "═══════════════════════════════════════════════════════════════════════════════\n"
        json_structure_text += "\n"
        json_structure_text += "This is a JSON file. JSON files use NESTED STRUCTURES, not flat CSV columns.\n"
        json_structure_text += "\n"
        json_structure_text += "EXAMPLE JSON STRUCTURE:\n"
        json_structure_text += "  {\n"
        json_structure_text += "    \"code_information\": [\n"
        json_structure_text += "      {\"code\": \"12345\", \"type\": \"CPT\"}\n"
        json_structure_text += "    ],\n"
        json_structure_text += "    \"drug_information\": {\n"
        json_structure_text += "      \"unit\": \"200\",\n"
        json_structure_text += "      \"type\": \"ML\"\n"
        json_structure_text += "    }\n"
        json_structure_text += "  }\n"
        json_structure_text += "\n"
        json_structure_text += "✅ CORRECT column names:\n"
        json_structure_text += "  - code_extraction.columns: [\"code_information\"]\n"
        json_structure_text += "  - code_extraction.columns: [\"drug_information\"]\n"
        json_structure_text += "\n"
        json_structure_text += "❌ WRONG column names (DO NOT USE):\n"
        json_structure_text += "  - code_extraction.columns: [\"code_information|type\"]  ← WRONG!\n"
        json_structure_text += "  - code_extraction.columns: [\"drug_information|type\"]  ← WRONG!\n"
        json_structure_text += "  - code_extraction.columns: [\"code_information|1\"]    ← WRONG!\n"
        json_structure_text += "\n"
        json_structure_text += "KEY RULE: Use ONLY the top-level key name. The extraction code automatically\n"
        json_structure_text += "handles nested structures. You do NOT need to specify nested paths with \"|\".\n"
        json_structure_text += "\n"
        json_structure_text += "ACTUAL SAMPLE RECORD FROM THIS FILE:\n"
        # Show a simplified version of the sample record
        if isinstance(sample_record, dict):
            sample_str = json.dumps(sample_record, indent=2)
            # Truncate if too long
            if len(sample_str) > 500:
                sample_str = sample_str[:500] + "\n  ... (truncated)"
            json_structure_text += sample_str
        json_structure_text += "\n"
        json_structure_text += "═══════════════════════════════════════════════════════════════════════════════\n"
        json_structure_text += "\n"
    
    # Detected patterns summary
    patterns_text = ""
    if detected_patterns:
        if detected_patterns.get("code_columns"):
            patterns_text += f"Code columns detected: {detected_patterns['code_columns']}\n"
        if detected_patterns.get("description_columns"):
            patterns_text += f"Description columns: {detected_patterns['description_columns']}\n"
        if detected_patterns.get("price_columns"):
            patterns_text += f"Price columns: {len(detected_patterns['price_columns'])} found (filtered to exclude empty/placeholder columns)\n"
            
            # Show price columns with fill rate information (CRITICAL for AI selection)
            price_cols_detailed = detected_patterns.get("price_columns_detailed", [])
            if price_cols_detailed:
                patterns_text += "\n⚠️ PRICE COLUMN SELECTION - USE FILL RATES:\n"
                patterns_text += "Columns are sorted by fill rate (highest first). Prefer columns with higher fill rates!\n"
                for col_info in price_cols_detailed[:10]:  # Top 10
                    col_name = col_info.get("column_name", "unknown")
                    fill_rate = col_info.get("fill_rate", 0)
                    likely_type = col_info.get("likely_type", "unknown")
                    is_placeholder = col_info.get("is_placeholder_heavy", False)
                    
                    status = "✅ GOOD" if fill_rate > 0.1 else "⚠️ LOW FILL"
                    if is_placeholder:
                        status = "❌ PLACEHOLDER"
                    
                    patterns_text += f"  {status} {col_name}: {fill_rate*100:.1f}% filled ({likely_type})\n"
                    
                    # Show sample values if available
                    samples = col_info.get("sample_values", [])
                    if samples and len(samples) > 0:
                        samples_str = ', '.join(str(s)[:20] for s in samples[:3])
                        patterns_text += f"    Samples: [{samples_str}]\n"
            else:
                # Fallback to simple list if detailed info not available
                price_cols = detected_patterns['price_columns'][:5]
                patterns_text += f"Price column examples: {price_cols}\n"
        if detected_patterns.get("has_payer_column"):
            patterns_text += "Has payer column: Yes (TALL format indicator)\n"
        if detected_patterns.get("has_plan_column"):
            patterns_text += "Has plan column: Yes\n"
        # Header-style payer format detection (from analyze_csv.py)
        if detected_patterns.get("has_header_style_payers"):
            header_cols = detected_patterns.get("header_style_payer_columns", [])
            patterns_text += f"\n⚠️ HEADER STYLE DETECTED: Found {len(header_cols)} columns with payer names in column names:\n"
            patterns_text += f"Examples: {header_cols[:5]}\n"
            patterns_text += "This indicates payer_style should be 'header', NOT 'column'!\n"
        # JSON-specific patterns - ENHANCED WARNINGS
        if format_type == 'json':
            patterns_text += "\n"
            patterns_text += "🚨🚨🚨 JSON FILE DETECTED - CRITICAL RULES 🚨🚨🚨\n"
            patterns_text += "\n"
            if detected_patterns.get("has_nested_charges"):
                patterns_text += "✓ Has nested charge structure: Yes\n"
            if detected_patterns.get("has_nested_code_info"):
                patterns_text += "✓ NESTED CODE STRUCTURE DETECTED: code_information is a nested array/list\n"
                if detected_patterns.get("code_info_keys"):
                    patterns_text += f"  Nested keys inside code_information: {detected_patterns['code_info_keys']}\n"
                patterns_text += "  → Use column name: \"code_information\" (NOT \"code_information|type\" or \"code_information|1\")\n"
            if detected_patterns.get("has_drug_info"):
                patterns_text += "✓ DRUG INFORMATION DETECTED: drug_information is a nested object\n"
                patterns_text += "  → Use column name: \"drug_information\" (NOT \"drug_information|type\" or \"drug_information|unit\")\n"
            if detected_patterns.get("record_keys"):
                patterns_text += f"  Top-level record keys: {detected_patterns['record_keys'][:10]}\n"
                patterns_text += "  → These are the ONLY valid column names. Use them EXACTLY as shown.\n"
            patterns_text += "\n"
            patterns_text += "⚠️ REMEMBER: JSON files use simple key names. NO \"|\" separators!\n"
            patterns_text += "   The extraction code handles nested structures automatically.\n"
            patterns_text += "\n"
    
    # List ALL columns for code detection (define early for use in fallback check)
    all_columns = profile.get("columns", [])
    code_like_cols = [c for c in all_columns if 'code' in c.lower()]
    
    # Fallback: Check for header-style payer format if not in detected_patterns (for backwards compatibility)
    if not detected_patterns.get("has_header_style_payers"):
        header_style_indicators = []
        for col in all_columns:
            col_str = str(col)
            # Look for columns with payer names embedded (e.g., standard_charge|Aetna|...)
            if ('negotiated_dollar' in col_str or 'estimated_amount' in col_str) and '|' in col_str:
                parts = col_str.split('|')
                if len(parts) >= 2:
                    # Second part might be payer name
                    potential_payer = parts[1].strip()
                    # Exclude common non-payer values
                    if potential_payer and potential_payer.lower() not in ['gross', 'discounted_cash', 'min', 'max', 'negotiated_dollar', 'estimated_amount']:
                        header_style_indicators.append(col_str)
        
        if header_style_indicators:
            patterns_text += f"\n⚠️ HEADER STYLE DETECTED: Found {len(header_style_indicators)} columns with payer names in column names:\n"
            patterns_text += f"Examples: {header_style_indicators[:5]}\n"
            patterns_text += "This indicates payer_style should be 'header', NOT 'column'!\n"
    
    # Get config template values (already determined by Phase 2)
    template_format_type = config_template.get('format_type', format_type)
    template_header_row = config_template.get('header_row', 0)
    template_encoding = config_template.get('encoding', 'utf-8')
    template_payer_style = config_template.get('price_extraction', {}).get('payer_style')
    template_description = config_template.get('description_column')
    template_setting_primary = config_template.get('setting_extraction', {}).get('primary')
    
    prompt = f"""You are a data engineer completing an ingestion config for hospital pricing files.
Phase 2 has already determined all structural values. You only need to refine semantic mappings.

{json_structure_text}═══════════════════════════════════════════════════════════════════════════════
HOSPITAL INFORMATION
═══════════════════════════════════════════════════════════════════════════════
Hospital: {hospital_name}
Total Rows: {total_rows:,}
Format Type: {(format_type or "unknown").upper()}

═══════════════════════════════════════════════════════════════════════════════
CONFIG TEMPLATE (from Phase 2 - DO NOT CHANGE THESE VALUES)
═══════════════════════════════════════════════════════════════════════════════
{json.dumps(config_template, indent=2)}

CRITICAL: All values in the template above are DETERMINISTIC and CORRECT.
DO NOT change: format_type, header_row, encoding, payer_style, payer_column, setting_extraction.primary, setting_extraction.fallback

═══════════════════════════════════════════════════════════════════════════════
DATA ANALYSIS
═══════════════════════════════════════════════════════════════════════════════
ALL COLUMNS WITH "CODE" IN NAME:
{code_like_cols}

COLUMN ANALYSIS:
{column_text}

DETECTED PATTERNS:
{patterns_text}

═══════════════════════════════════════════════════════════════════════════════
YOUR TASK - COMPLETE THE CONFIG TEMPLATE
═══════════════════════════════════════════════════════════════════════════════

The config template above has all deterministic values pre-filled. You need to:

1. **REFINE code_extraction.columns**: 
   - Template has detected columns, but you should verify and list ALL code columns
   - 🚨 FOR JSON FILES: Use ONLY simple top-level key names (e.g., ["code_information"])
     → DO NOT use "|" separators (e.g., NOT "code_information|type")
     → DO NOT try to access nested fields with "|" (the extraction code handles this automatically)
     → Examples: ["code_information"], ["drug_information"], ["procedure"]
   - FOR CSV FILES: List ALL code columns with "|" separators (e.g., ["code|1", "code|2", "code|3"])
   - Add type_columns if code types are in separate columns (e.g., ["code|1|type"]) - ONLY for CSV files

2. **REFINE description_column**:
   - Template has a best guess, verify it's the best match
   - Look for columns with: "description", "desc", "name", "procedure", "service"

3. **COMPLETE price_extraction**:
   - Template has payer_style and payer_column (if detected) - DO NOT CHANGE
   - Add: price_column, gross_column, cash_column, plan_column (if found)
   - ⚠️ CRITICAL: Check fill rates above! Prefer columns with HIGHEST fill rate
   - ⚠️ DO NOT select columns marked as "❌ PLACEHOLDER" or "⚠️ LOW FILL" (< 10%)
   - For header style: price columns are like "standard_charge|PayerName|...|negotiated_dollar"
   - For column style: single price_column with payer_column already set
   - If multiple price columns exist, pick the one with the highest fill rate that isn't a placeholder

4. **VERIFY setting_extraction**:
   - Template has primary and fallback - verify they're correct
   - For JSON: These are key names (may be nested in standard_charges)

5. **ADD notes_column** (if available):
   - Look for columns with "note", "comment", "additional" in name

6. **CALCULATE confidence**: Rate 0.0 to 1.0 based on how clear the mappings are

═══════════════════════════════════════════════════════════════════════════════
CRITICAL RULES - READ CAREFULLY
═══════════════════════════════════════════════════════════════════════════════

🚨 JSON FILE RULES (if format_type == "json"):
  1. code_extraction.columns MUST use simple key names ONLY
     ✅ CORRECT: ["code_information"], ["drug_information"]
     ❌ WRONG: ["code_information|type"], ["drug_information|unit"], ["code_information|1"]
  2. DO NOT use "|" separators in column names - this is for CSV files only
  3. The extraction code automatically handles nested JSON structures
  4. You only need to specify the top-level key name

📋 CSV FILE RULES (if format_type == "tall" or "wide"):
  1. code_extraction.columns MUST use "|" separators for multi-column codes
     ✅ CORRECT: ["code|1", "code|2", "code|3"]
  2. type_columns can use "|" separators: ["code|1|type"]

🔒 TEMPLATE VALUES (DO NOT CHANGE):
- DO NOT change any values from the template that are already set
- payer_style and payer_column in template are CORRECT - use them as-is
- setting_extraction.primary and fallback in template are CORRECT - use them as-is

✅ VALIDATION CHECKLIST (before outputting):
- [ ] If format_type == "json": No "|" in any code_extraction.columns
- [ ] If format_type == "json": All column names match top-level keys from record_keys
- [ ] description_column is set
- [ ] price_extraction is complete
- [ ] setting_extraction.primary is not None

═══════════════════════════════════════════════════════════════════════════════
OUTPUT
═══════════════════════════════════════════════════════════════════════════════

Output the COMPLETE config JSON. Start with the template and fill in/refine the missing parts.
All template values must be preserved exactly as shown.

OUTPUT ONLY VALID JSON. No markdown, no explanation, no comments."""

    return prompt


def parse_ai_response(response_text):
    """
    Parse the AI's JSON response, handling common issues.
    """
    # Clean up the response
    text = response_text.strip()
    
    # Remove markdown code blocks if present
    if text.startswith("```"):
        lines = text.split("\n")
        # Find the JSON content between ``` markers
        start_idx = 1 if lines[0].startswith("```") else 0
        end_idx = len(lines)
        for i, line in enumerate(lines[1:], 1):
            if line.strip() == "```":
                end_idx = i
                break
        text = "\n".join(lines[start_idx:end_idx])
    
    # Try to parse as JSON
    try:
        config = json.loads(text)
        return config, None
    except json.JSONDecodeError as e:
        # Try to find JSON object in the text
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1:
            try:
                config = json.loads(text[start:end+1])
                return config, None
            except:
                pass
        return None, f"JSON parse error: {str(e)}"


def validate_config(config):
    """
    Validate that the config has required fields for the enhanced schema.
    Returns (is_valid, error_message)
    """
    # Check format type first
    if config.get('format_type') not in ['tall', 'wide', 'json']:
        return False, f"Invalid format_type: {config.get('format_type')}"
    
    # Check for description column
    if not config.get('description_column'):
        return False, "Missing description_column"
    
    # Check code_extraction structure
    code_ext = config.get('code_extraction')
    if not code_ext:
        # Backward compatibility: check for old-style code_column
        if not config.get('code_column'):
            return False, "Missing code_extraction or code_column"
    else:
        # New schema: validate code_extraction
        if not code_ext.get('columns') or len(code_ext.get('columns', [])) == 0:
            return False, "code_extraction.columns is empty"
        
        # CRITICAL: For JSON files, reject CSV-style column names (with | separators)
        format_type = config.get('format_type', 'tall')
        if format_type == 'json':
            code_columns = code_ext.get('columns', [])
            csv_style_cols = [col for col in code_columns if '|' in str(col)]
            if csv_style_cols:
                return False, f"JSON files cannot use CSV-style column names with '|' separators. Found: {csv_style_cols}. Use simple key names like 'code_information' instead."
        
        # CRITICAL: Reject configs where type columns are mixed into columns array
        # Type columns should be in type_columns, not in columns
        code_columns = code_ext.get('columns', [])
        type_columns_in_columns = []
        for col in code_columns:
            col_str = str(col)
            # Check if this looks like a type column
            if '|type' in col_str.lower() or col_str.endswith('|type') or col_str.endswith('_type'):
                type_columns_in_columns.append(col)
        
        if type_columns_in_columns:
            return False, f"Type columns found in code_extraction.columns array: {type_columns_in_columns}. Type columns must be in type_columns array, not in columns array."
    
    # Check price_extraction structure
    price_ext = config.get('price_extraction')
    if not price_ext:
        return False, "Missing price_extraction"
    
    # For tall/wide, we need payer_style
    if config.get('format_type') in ['tall', 'wide']:
        if not price_ext.get('payer_style') and not price_ext.get('type'):
            return False, "price_extraction missing payer_style or type"
    
    # Validate setting_extraction - primary must never be None
    setting_ext = config.get('setting_extraction', {})
    if setting_ext.get('primary') is None:
        return False, "setting_extraction.primary cannot be None - must provide key name (e.g., 'setting', 'billing_class')"
    
    return True, None


def generate_config_for_hospital(hospital_id, hospital_name, profile):
    """
    Generate ingestion config for a single hospital using Llama 3.
    Returns (config, error)
    """
    prompt = create_prompt(profile, hospital_name)
    
    try:
        # Call Ollama with optimized settings for accuracy
        response = ollama.generate(
            model=MODEL_NAME,
            prompt=prompt,
            format='json',  # Request JSON output
            options={
                'temperature': 0.05,        # Very low for maximum consistency
                'num_predict': 3000,        # More tokens = more room to think/reason
                'top_p': 0.9,               # Focus on most likely tokens (nucleus sampling)
                'top_k': 40,                # Consider top 40 token options
                'repeat_penalty': 1.15,     # Slight penalty to avoid repetition/loops
                'num_ctx': 8192,            # Larger context window for longer prompts
            }
        )
        
        response_text = response.get('response', '')
        
        # Parse the response
        config, parse_error = parse_ai_response(response_text)
        
        if parse_error:
            return None, parse_error
        
        # Auto-fix JSON column names if needed (before validation)
        format_type = profile.get("format_type", "unknown")
        config, json_fixes = auto_fix_json_column_names(config, format_type)
        if json_fixes:
            print(f"  ⚠️  Auto-fixed JSON column names: {', '.join(json_fixes)}")
        
        # Auto-fix type columns that are incorrectly placed in columns array (before validation)
        # This works for ALL format types (CSV and JSON)
        config, type_fixes = auto_fix_type_columns(config)
        if type_fixes:
            print(f"  ⚠️  Auto-fixed type columns: {', '.join(type_fixes)}")
        
        # Validate the config
        is_valid, validation_error = validate_config(config)
        if not is_valid:
            return None, validation_error
        
        # MERGE with config template (template values take precedence)
        # This ensures all deterministic values from Phase 2 are preserved
        config_template = profile.get('config_template', {})
        # Fix template for JSON files (defensive - template should already be correct from Phase 2)
        config_template, _ = validate_and_fix_template_for_json(config_template, format_type)
        if config_template:
            # Merge template into config (template values override AI output)
            # This ensures format_type, header_row, encoding, payer_style, etc. are correct
            for key, value in config_template.items():
                if key == 'code_extraction' and isinstance(value, dict):
                    # Merge code_extraction dict - template has proper code/type column separation
                    if 'code_extraction' not in config:
                        config['code_extraction'] = {}
                    # Template columns and type_columns are correct - use them
                    if 'columns' in value:
                        config['code_extraction']['columns'] = value['columns']
                    if 'type_columns' in value:
                        config['code_extraction']['type_columns'] = value['type_columns']
                    if 'priority' in value:
                        config['code_extraction']['priority'] = value['priority']
                    if 'auto_normalize' in value:
                        config['code_extraction']['auto_normalize'] = value['auto_normalize']
                    # Merge other code_extraction fields from AI (if any)
                    for ai_key, ai_value in config.get('code_extraction', {}).items():
                        if ai_key not in ['columns', 'type_columns', 'priority', 'auto_normalize']:
                            config['code_extraction'][ai_key] = ai_value
                elif key == 'price_extraction' and isinstance(value, dict):
                    # Merge price_extraction dict
                    if 'price_extraction' not in config:
                        config['price_extraction'] = {}
                    # Template payer_style and payer_column are correct - use them
                    if 'payer_style' in value:
                        config['price_extraction']['payer_style'] = value['payer_style']
                    if 'payer_column' in value:
                        config['price_extraction']['payer_column'] = value['payer_column']
                    if 'type' in value:
                        config['price_extraction']['type'] = value['type']
                    # Merge other price_extraction fields from AI
                    for ai_key, ai_value in config.get('price_extraction', {}).items():
                        if ai_key not in ['payer_style', 'payer_column', 'type']:
                            config['price_extraction'][ai_key] = ai_value
                elif key == 'setting_extraction' and isinstance(value, dict):
                    # Merge setting_extraction dict
                    if 'setting_extraction' not in config:
                        config['setting_extraction'] = {}
                    # Template primary and fallback are correct - use them
                    if 'primary' in value:
                        config['setting_extraction']['primary'] = value['primary']
                    if 'fallback' in value:
                        config['setting_extraction']['fallback'] = value['fallback']
                    if 'default' in value:
                        config['setting_extraction']['default'] = value['default']
                else:
                    # For other keys, template takes precedence
                    config[key] = value
            
            # Validate that critical template values were preserved
            if config.get('format_type') != config_template.get('format_type'):
                return None, f"AI changed format_type from template value {config_template.get('format_type')} to {config.get('format_type')}. This is not allowed."
            if config.get('header_row') != config_template.get('header_row'):
                return None, f"AI changed header_row from template value {config_template.get('header_row')} to {config.get('header_row')}. This is not allowed."
            if config.get('encoding') != config_template.get('encoding'):
                return None, f"AI changed encoding from template value {config_template.get('encoding')} to {config.get('encoding')}. This is not allowed."
        
        # Add metadata
        config['_hospital_id'] = hospital_id
        config['_generated_at'] = datetime.now().isoformat()
        config['_model'] = MODEL_NAME
        
        return config, None
        
    except Exception as e:
        error_msg = str(e)
        if "connection" in error_msg.lower() or "refused" in error_msg.lower():
            return None, "Ollama not running. Start with: ollama serve"
        return None, error_msg


def process_hospital(hospital_id, analysis_info, config_manifest):
    """
    Process a single hospital: generate config from profile.
    Returns status string.
    """
    hospital_name = analysis_info.get("name", "Unknown")
    
    # Check if already processed
    if hospital_id in config_manifest["configs"]:
        status = config_manifest["configs"][hospital_id].get("status")
        if status == "completed":
            return "skipped"
    
    print(f"\n{'='*60}")
    print(f"Generating config: {hospital_name}")
    
    # Load profile
    profile = load_profile(hospital_id)
    if not profile:
        print("  ⚠️  Profile not found")
        config_manifest["configs"][hospital_id] = {
            "name": hospital_name,
            "status": "profile_not_found",
            "timestamp": datetime.now().isoformat()
        }
        return "failed"
    
    print(f"  Format: {profile.get('format_type', 'unknown')}")
    print(f"  Columns: {profile.get('total_columns', 'N/A')}")
    print("  🤖 Calling Llama 3...")
    
    # Generate config
    config, error = generate_config_for_hospital(hospital_id, hospital_name, profile)
    
    if error:
        print(f"  ❌ Failed: {error}")
        config_manifest["configs"][hospital_id] = {
            "name": hospital_name,
            "status": "failed",
            "error": error,
            "timestamp": datetime.now().isoformat()
        }
        return "failed"
    
    # Save config with sanitized hospital name (matching downloads folder)
    # Get sanitized name from analysis manifest
    analysis_manifest = load_analysis_manifest()
    analysis_info = analysis_manifest.get("analyses", {}).get(hospital_id, {})
    sanitized_name = analysis_info.get("sanitized_name")
    
    if not sanitized_name:
        # Fallback: sanitize the name from config
        sanitized_name = sanitize_filename(hospital_name)
    
    config_file = CONFIGS_DIR / f"{sanitized_name}.json"
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    # Update manifest with enhanced schema details
    code_ext = config.get("code_extraction", {})
    price_ext = config.get("price_extraction", {})
    
    config_manifest["configs"][hospital_id] = {
        "name": hospital_name,
        "sanitized_name": sanitized_name,  # Store for file lookup
        "status": "completed",
        "config_file": str(config_file),
        "format_type": config.get("format_type"),
        "code_columns_count": len(code_ext.get("columns", [])) if code_ext else 1,
        "payer_style": price_ext.get("payer_style", price_ext.get("type")),
        "confidence": config.get("confidence", "N/A"),
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"  ✅ Config generated!")
    print(f"     Format: {config.get('format_type')}")
    
    # Show code extraction info
    code_ext = config.get('code_extraction', {})
    if code_ext and code_ext.get('columns'):
        print(f"     Code columns: {code_ext['columns'][:3]}{'...' if len(code_ext.get('columns', [])) > 3 else ''}")
    else:
        print(f"     Code column: {config.get('code_column', 'N/A')}")
    
    print(f"     Description: {config.get('description_column')}")
    
    # Show price extraction style
    price_ext = config.get('price_extraction', {})
    if price_ext:
        payer_style = price_ext.get('payer_style', price_ext.get('type', 'N/A'))
        print(f"     Payer style: {payer_style}")
    
    print(f"     Confidence: {config.get('confidence', 'N/A')}")
    
    return "completed"


def main():
    """Main config generation orchestrator."""
    print("=" * 60)
    print("  AI CONFIG GENERATOR - Phase 3")
    print("  Using Llama 3 via Ollama")
    print("=" * 60)
    
    # Test Ollama connection
    print("\nTesting Ollama connection...")
    try:
        ollama.list()
        print("✅ Ollama is running")
    except Exception as e:
        print(f"❌ Ollama not available: {e}")
        print("\nPlease start Ollama with: ollama serve")
        print("Then run this script again.")
        sys.exit(1)
    
    # Load analysis manifest
    analysis_manifest = load_analysis_manifest()
    
    # Get completed analyses
    completed_analyses = {
        k: v for k, v in analysis_manifest.get("analyses", {}).items()
        if v.get("status") == "completed"
    }
    
    print(f"\nFound {len(completed_analyses)} analyzed hospitals")
    
    # Create configs directory
    CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load/create config manifest
    config_manifest = load_config_manifest()
    config_manifest["stats"]["total"] = len(completed_analyses)
    
    # Count existing configs
    already_done = sum(1 for h_id in completed_analyses 
                       if config_manifest["configs"].get(h_id, {}).get("status") == "completed")
    print(f"Already configured: {already_done}")
    print(f"Remaining: {len(completed_analyses) - already_done}")
    
    if already_done == len(completed_analyses):
        print("\n✅ All hospitals already have configs!")
        print(f"Configs saved in: {CONFIGS_DIR}")
        return
    
    print(f"\nStarting config generation (delay: {DELAY_BETWEEN_REQUESTS}s between requests)")
    print("Press Ctrl+C to stop (progress is saved)\n")
    
    # Process each hospital
    stats = {"completed": 0, "failed": 0, "skipped": 0}
    
    try:
        for i, (hospital_id, analysis_info) in enumerate(completed_analyses.items()):
            print(f"[{i+1}/{len(completed_analyses)}]", end="")
            
            status = process_hospital(hospital_id, analysis_info, config_manifest)
            stats[status] = stats.get(status, 0) + 1
            
            # Save manifest periodically
            if (i + 1) % 5 == 0:
                config_manifest["stats"] = stats
                save_config_manifest(config_manifest)
            
            # Delay between requests (skip if skipped)
            if status != "skipped":
                time.sleep(DELAY_BETWEEN_REQUESTS)
                
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Progress saved.")
    
    # Final save
    config_manifest["stats"] = stats
    save_config_manifest(config_manifest)
    
    # Summary
    print("\n" + "=" * 60)
    print("  CONFIG GENERATION SUMMARY")
    print("=" * 60)
    print(f"  Total Hospitals: {len(completed_analyses)}")
    print(f"  ✅ Completed: {stats.get('completed', 0)}")
    print(f"  ⏭️  Skipped (already done): {stats.get('skipped', 0)}")
    print(f"  ❌ Failed: {stats.get('failed', 0)}")
    print(f"\n  Configs saved to: {CONFIGS_DIR}")
    print(f"  Manifest saved to: {CONFIG_MANIFEST}")
    print("=" * 60)


if __name__ == "__main__":
    main()

