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
MODEL_NAME = "llama3.1"  # Using llama3.1 (installed on this system)
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


def load_profile(hospital_id):
    """Load a hospital's analysis profile."""
    profile_file = PROFILES_DIR / f"{hospital_id}.json"
    if not profile_file.exists():
        return None
    
    with open(profile_file, 'r') as f:
        return json.load(f)


def create_prompt(profile, hospital_name):
    """
    Create a prompt for Llama 3 to generate an ingestion config.
    Enhanced schema with full feature parity to existing ingest_tall/ingest_wide scripts.
    """
    # Summarize the profile for the AI
    format_type = profile.get("format_type", "unknown")
    columns = profile.get("columns", [])
    column_analyses = profile.get("column_analyses", [])
    detected_patterns = profile.get("detected_patterns", {})
    total_rows = profile.get("total_rows") or profile.get("total_records") or 0
    
    # Build column summary
    column_summary = []
    for col in column_analyses[:40]:  # Increased limit for better context
        col_info = f"- {col['column_name']}: {col['likely_type']} ({col['inferred_purpose']}), {col['fill_rate']*100:.0f}% filled"
        if col.get('sample_values'):
            samples = ', '.join(str(v)[:30] for v in col['sample_values'][:3])
            col_info += f", samples: [{samples}]"
        column_summary.append(col_info)
    
    column_text = "\n".join(column_summary) if column_summary else "No column analysis available"
    
    # Detected patterns summary
    patterns_text = ""
    if detected_patterns:
        if detected_patterns.get("code_columns"):
            patterns_text += f"Code columns detected: {detected_patterns['code_columns']}\n"
        if detected_patterns.get("description_columns"):
            patterns_text += f"Description columns: {detected_patterns['description_columns']}\n"
        if detected_patterns.get("price_columns"):
            patterns_text += f"Price columns: {len(detected_patterns['price_columns'])} found\n"
            # Show some price column examples
            price_cols = detected_patterns['price_columns'][:5]
            patterns_text += f"Price column examples: {price_cols}\n"
        if detected_patterns.get("has_payer_column"):
            patterns_text += "Has payer column: Yes (TALL format indicator)\n"
        if detected_patterns.get("has_plan_column"):
            patterns_text += "Has plan column: Yes\n"
        # JSON-specific patterns
        if detected_patterns.get("has_nested_charges"):
            patterns_text += "Has nested charge structure: Yes\n"
        if detected_patterns.get("record_keys"):
            patterns_text += f"Record keys: {detected_patterns['record_keys'][:10]}\n"
    
    # List ALL columns for code detection
    all_columns = profile.get("columns", [])
    code_like_cols = [c for c in all_columns if 'code' in c.lower()]
    
    prompt = f"""You are a data engineer creating ingestion configs for hospital pricing files. Analyze carefully and generate a comprehensive JSON configuration.

HOSPITAL: {hospital_name}
FORMAT TYPE: {format_type}
TOTAL ROWS: {total_rows:,}
HEADER ROW: {profile.get('header_row', 0)}
ENCODING: {profile.get('encoding', 'utf-8')}

ALL COLUMNS WITH "CODE" IN NAME:
{code_like_cols}

COLUMN ANALYSIS:
{column_text}

DETECTED PATTERNS:
{patterns_text}

Generate a JSON config with this EXACT structure:

{{
  "hospital_name": "{hospital_name}",
  "format_type": "tall" or "wide" or "json",
  "header_row": number (0-indexed),
  "encoding": "utf-8" or "iso-8859-1",
  
  "code_extraction": {{
    "columns": ["code|1", "code|2", ...] or ["code", "hcpcs_code", ...],
    "type_columns": ["code|1|type", "code|2|type", ...] or null if types are not in separate columns,
    "priority": ["CPT", "HCPCS", "MS-DRG", "APR-DRG", "NDC", "CDM", "Local"],
    "auto_normalize": true
  }},
  
  "description_column": "description" or similar,
  
  "setting_extraction": {{
    "primary": "setting" or "billing_class" or similar,
    "fallback": "billing_class" or null,
    "default": "UNKNOWN"
  }},
  
  "price_extraction": {{
    "type": "tall" or "wide",
    "payer_style": "column" or "header",
    "payer_column": "payer_name" or null (for TALL with payer in column),
    "plan_column": "plan_name" or null,
    "price_column": "standard_charge|negotiated_dollar" or similar,
    "gross_column": "standard_charge|gross" or similar,
    "cash_column": "standard_charge|discounted_cash" or similar,
    "sibling_columns": ["negotiated_algorithm", "methodology", "negotiated_percentage"],
    "price_column_pattern": "standard_charge|*|*|negotiated_dollar" (for WIDE format)
  }},
  
  "skip_rules": {{
    "placeholder_threshold": 99999999,
    "formula_patterns": ["Formula", "algorithm"],
    "empty_code_skip": true
  }},
  
  "notes_column": "additional_generic_notes" or null,
  
  "confidence": 0.0 to 1.0
}}

RULES:
1. For "payer_style": Use "column" if there's a payer_name column (each row has payer). Use "header" if payers are encoded in column names (standard_charge|Aetna|PPO|negotiated_dollar).
2. For "code_extraction.columns": List ALL code columns found (code|1, code|2, code|3, code|4, code|5 or equivalent)
3. For JSON format files: Set format_type to "json" and adapt price_extraction accordingly
4. Look for code type columns that end in "|type" (e.g., code|1|type contains "CPT", "HCPCS", etc.)
5. If no type columns exist, set type_columns to null (we'll infer from code format)

OUTPUT ONLY VALID JSON. No markdown, no explanation."""

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
    
    # Check price_extraction structure
    price_ext = config.get('price_extraction')
    if not price_ext:
        return False, "Missing price_extraction"
    
    # For tall/wide, we need payer_style
    if config.get('format_type') in ['tall', 'wide']:
        if not price_ext.get('payer_style') and not price_ext.get('type'):
            return False, "price_extraction missing payer_style or type"
    
    return True, None


def generate_config_for_hospital(hospital_id, hospital_name, profile):
    """
    Generate ingestion config for a single hospital using Llama 3.
    Returns (config, error)
    """
    prompt = create_prompt(profile, hospital_name)
    
    try:
        # Call Ollama
        response = ollama.generate(
            model=MODEL_NAME,
            prompt=prompt,
            format='json',  # Request JSON output
            options={
                'temperature': 0.1,  # Low temperature for consistent output
                'num_predict': 2000,  # Max tokens
            }
        )
        
        response_text = response.get('response', '')
        
        # Parse the response
        config, parse_error = parse_ai_response(response_text)
        
        if parse_error:
            return None, parse_error
        
        # Validate the config
        is_valid, validation_error = validate_config(config)
        if not is_valid:
            return None, validation_error
        
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
    
    # Save config
    config_file = CONFIGS_DIR / f"{hospital_id}.json"
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    # Update manifest with enhanced schema details
    code_ext = config.get("code_extraction", {})
    price_ext = config.get("price_extraction", {})
    
    config_manifest["configs"][hospital_id] = {
        "name": hospital_name,
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

