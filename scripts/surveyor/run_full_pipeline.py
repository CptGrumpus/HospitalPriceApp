#!/usr/bin/env python3
"""
Master Pipeline Script - Runs Phases 2, 3, and 4 in Sequence

This script orchestrates the AI Surveyor pipeline:
- Phase 2: analyze_csv.py - Analyzes downloaded files and generates profiles
- Phase 3: generate_config.py - Uses AI to generate ingestion configs
- Phase 4: preview_cards.py - Generates preview cards for validation

IMPORTANT: This script does NOT download files. Downloads are Phase 1 (separate).
Run download_all.py first if you need to download hospital files.

Usage:
    python3 scripts/surveyor/run_full_pipeline.py [--fresh] [--no-server]

Options:
    --fresh      Delete existing profiles and configs before running (fresh start)
    --no-server  Generate preview HTML but don't start the validation server

Output:
    - Detailed summary log in docs/pipeline_summary_TIMESTAMP.txt
    - Includes error messages, warnings, file paths, and debugging tips
"""

import sys
import subprocess
import argparse
from pathlib import Path
import shutil
import json
from datetime import datetime

# Configuration
DATA_DIR = Path(__file__).parent.parent.parent / "data"
PROFILES_DIR = DATA_DIR / "profiles"
CONFIGS_DIR = DATA_DIR / "configs"
DOWNLOADS_DIR = DATA_DIR / "downloads"
ANALYSIS_MANIFEST = PROFILES_DIR / "analysis_manifest.json"
CONFIG_MANIFEST = CONFIGS_DIR / "config_manifest.json"
DOWNLOAD_MANIFEST = DOWNLOADS_DIR / "download_manifest.json"
DOCS_DIR = Path(__file__).parent.parent.parent / "docs"

# Script paths (relative to project root)
SCRIPT_DIR = Path(__file__).parent
ANALYZE_SCRIPT = SCRIPT_DIR / "analyze_csv.py"
GENERATE_SCRIPT = SCRIPT_DIR / "generate_config.py"
PREVIEW_SCRIPT = SCRIPT_DIR / "preview_cards.py"


def print_header(text):
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")


def print_section(text):
    """Print a section divider."""
    print("\n" + "-" * 80)
    print(f"  {text}")
    print("-" * 80 + "\n")


def delete_profiles(fresh=False):
    """Delete existing profiles if fresh=True."""
    if not fresh:
        return
    
    print_section("🧹 CLEANING UP OLD PROFILES")
    
    deleted_count = 0
    if PROFILES_DIR.exists():
        for profile_file in PROFILES_DIR.glob("*.json"):
            if profile_file.name != "analysis_manifest.json":
                profile_file.unlink()
                deleted_count += 1
    
    # Reset analysis manifest
    if ANALYSIS_MANIFEST.exists():
        ANALYSIS_MANIFEST.unlink()
    
    print(f"  ✅ Deleted {deleted_count} profile files")
    print(f"  ✅ Reset analysis manifest")


def delete_configs(fresh=False):
    """Delete existing configs if fresh=True."""
    if not fresh:
        return
    
    print_section("🧹 CLEANING UP OLD CONFIGS")
    
    deleted_count = 0
    if CONFIGS_DIR.exists():
        for config_file in CONFIGS_DIR.glob("*.json"):
            if config_file.name != "config_manifest.json":
                config_file.unlink()
                deleted_count += 1
    
    # Reset config manifest
    if CONFIG_MANIFEST.exists():
        CONFIG_MANIFEST.unlink()
    
    print(f"  ✅ Deleted {deleted_count} config files")
    print(f"  ✅ Reset config manifest")


def run_phase(script_path, phase_name, phase_number):
    """Run a phase script and handle errors."""
    print_section(f"PHASE {phase_number}: {phase_name}")
    
    if not script_path.exists():
        print(f"  ❌ ERROR: Script not found: {script_path}")
        return False
    
    try:
        # Run the script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=Path(__file__).parent.parent.parent,  # Run from project root
            check=True,
            capture_output=False  # Show output in real-time
        )
        
        if result.returncode == 0:
            print(f"\n  ✅ Phase {phase_number} completed successfully")
            return True
        else:
            print(f"\n  ❌ Phase {phase_number} failed with exit code {result.returncode}")
            return False
            
    except subprocess.CalledProcessError as e:
        print(f"\n  ❌ Phase {phase_number} failed: {e}")
        return False
    except KeyboardInterrupt:
        print(f"\n\n  ⚠️  Phase {phase_number} interrupted by user")
        raise
    except Exception as e:
        print(f"\n  ❌ Phase {phase_number} error: {e}")
        return False


def generate_preview_html_only():
    """Generate preview HTML without starting the server."""
    print_section("PHASE 4: PREVIEW CARDS (HTML Generation Only)")
    
    # Import preview_cards functions directly
    sys.path.insert(0, str(SCRIPT_DIR))
    from preview_cards import load_config_manifest, generate_html
    
    try:
        manifest = load_config_manifest()
        print("  📊 Generating preview cards HTML...")
        
        html_content = generate_html(manifest)
        
        preview_file = DATA_DIR / "preview_cards.html"
        with open(preview_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"  ✅ Preview HTML saved to: {preview_file}")
        print(f"  💡 To view it, run: python3 {PREVIEW_SCRIPT}")
        return True
        
    except Exception as e:
        print(f"  ❌ Failed to generate preview HTML: {e}")
        return False


def get_phase2_summary():
    """Get detailed Phase 2 summary from analysis manifest."""
    summary = {
        'total_downloads': 0,
        'completed': 0,
        'failed': 0,
        'skipped': 0,
        'hospitals': {}
    }
    
    # Get download manifest to see what should have been processed
    if DOWNLOAD_MANIFEST.exists():
        with open(DOWNLOAD_MANIFEST, 'r') as f:
            download_manifest = json.load(f)
            completed_downloads = {
                k: v for k, v in download_manifest.get("downloads", {}).items()
                if v.get("status") == "completed"
            }
            summary['total_downloads'] = len(completed_downloads)
    
    # Get analysis manifest
    if ANALYSIS_MANIFEST.exists():
        with open(ANALYSIS_MANIFEST, 'r') as f:
            analysis_manifest = json.load(f)
            analyses = analysis_manifest.get("analyses", {})
            stats = analysis_manifest.get("stats", {})
            
            summary['completed'] = stats.get('completed', 0)
            summary['failed'] = stats.get('failed', 0)
            summary['skipped'] = stats.get('skipped', 0)
            
            # Get per-hospital details
            for hospital_id, analysis_info in analyses.items():
                status = analysis_info.get('status', 'unknown')
                name = analysis_info.get('name', 'Unknown')
                error = analysis_info.get('error')
                file_type = analysis_info.get('file_type', 'unknown')
                format_type = analysis_info.get('format_type', 'unknown')
                total_rows = analysis_info.get('total_rows', 0)
                total_cols = analysis_info.get('total_columns', 0)
                profile_file = analysis_info.get('profile_file')
                warnings_count = analysis_info.get('warnings', 0)
                errors_count = analysis_info.get('errors', 0)
                
                # Load profile to get actual warning/error messages if available
                warnings = []
                errors = []
                if profile_file and Path(profile_file).exists():
                    try:
                        with open(profile_file, 'r') as pf:
                            profile = json.load(pf)
                            warnings = profile.get('warnings', [])
                            errors = profile.get('errors', [])
                    except:
                        pass
                
                summary['hospitals'][hospital_id] = {
                    'name': name,
                    'status': status,
                    'error': error,
                    'file_type': file_type,
                    'format_type': format_type,
                    'total_rows': total_rows,
                    'total_columns': total_cols,
                    'profile_file': profile_file,
                    'warnings': warnings,
                    'errors': errors,
                    'warnings_count': warnings_count,
                    'errors_count': errors_count
                }
    
    return summary


def get_phase3_summary():
    """Get detailed Phase 3 summary from config manifest."""
    summary = {
        'total_analyses': 0,
        'completed': 0,
        'failed': 0,
        'skipped': 0,
        'hospitals': {}
    }
    
    # Get analysis manifest to see what should have been processed
    if ANALYSIS_MANIFEST.exists():
        with open(ANALYSIS_MANIFEST, 'r') as f:
            analysis_manifest = json.load(f)
            completed_analyses = {
                k: v for k, v in analysis_manifest.get("analyses", {}).items()
                if v.get("status") == "completed"
            }
            summary['total_analyses'] = len(completed_analyses)
    
    # Get config manifest
    if CONFIG_MANIFEST.exists():
        with open(CONFIG_MANIFEST, 'r') as f:
            config_manifest = json.load(f)
            configs = config_manifest.get("configs", {})
            stats = config_manifest.get("stats", {})
            
            summary['completed'] = stats.get('completed', 0)
            summary['failed'] = stats.get('failed', 0)
            summary['skipped'] = stats.get('skipped', 0)
            
            # Get per-hospital details
            for hospital_id, config_info in configs.items():
                status = config_info.get('status', 'unknown')
                name = config_info.get('name', 'Unknown')
                error = config_info.get('error')
                format_type = config_info.get('format_type', 'unknown')
                payer_style = config_info.get('payer_style', 'unknown')
                confidence = config_info.get('confidence', 'N/A')
                
                summary['hospitals'][hospital_id] = {
                    'name': name,
                    'status': status,
                    'error': error,
                    'format_type': format_type,
                    'payer_style': payer_style,
                    'confidence': confidence
                }
    
    return summary


def get_pipeline_stats():
    """Get statistics about the pipeline results."""
    stats = {
        'profiles': 0,
        'configs': 0,
        'validated': 0,
        'rejected': 0,
        'pending': 0
    }
    
    # Count profiles
    if PROFILES_DIR.exists():
        stats['profiles'] = len(list(PROFILES_DIR.glob("*.json"))) - 1  # Exclude manifest
    
    # Count configs and validation status
    if CONFIG_MANIFEST.exists():
        with open(CONFIG_MANIFEST, 'r') as f:
            manifest = json.load(f)
            configs = manifest.get('configs', {})
            stats['configs'] = len(configs)
            
            for config_info in configs.values():
                validated = config_info.get('validated')
                if validated == True:
                    stats['validated'] += 1
                elif validated == False:
                    stats['rejected'] += 1
                else:
                    stats['pending'] += 1
    
    return stats


def write_summary_log(start_time, phase2_summary, phase3_summary, final_stats, args):
    """Write a comprehensive summary log to docs folder."""
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = DOCS_DIR / f"pipeline_summary_{timestamp}.txt"
    
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("  AI SURVEYOR PIPELINE - EXECUTION SUMMARY\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Execution Date: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Completed Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Fresh Start: {'Yes' if args.fresh else 'No'}\n")
        f.write(f"Server Started: {'No' if args.no_server else 'Yes'}\n")
        f.write("\n" + "=" * 80 + "\n\n")
        
        # Phase 2 Summary
        f.write("PHASE 2: FILE ANALYSIS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Downloads Available: {phase2_summary['total_downloads']}\n")
        f.write(f"✅ Completed: {phase2_summary['completed']}\n")
        f.write(f"⏭️  Skipped: {phase2_summary['skipped']}\n")
        f.write(f"❌ Failed: {phase2_summary['failed']}\n")
        f.write(f"Missing: {phase2_summary['total_downloads'] - phase2_summary['completed'] - phase2_summary['skipped'] - phase2_summary['failed']}\n\n")
        
        # Phase 2 Failed Hospitals
        failed_phase2 = {hid: info for hid, info in phase2_summary['hospitals'].items() 
                        if info['status'] == 'failed'}
        if failed_phase2:
            f.write("❌ PHASE 2 FAILED HOSPITALS:\n")
            for hospital_id, info in sorted(failed_phase2.items(), key=lambda x: x[1]['name']):
                f.write(f"  - {info['name']} ({hospital_id})\n")
                f.write(f"    Status: {info['status']}\n")
                f.write(f"    File Type: {info.get('file_type', 'unknown')}\n")
                
                # Get file path from download manifest
                if DOWNLOAD_MANIFEST.exists():
                    with open(DOWNLOAD_MANIFEST, 'r') as df:
                        download_manifest = json.load(df)
                        download_info = download_manifest.get("downloads", {}).get(hospital_id, {})
                        file_path = download_info.get('file_path', 'N/A')
                        f.write(f"    File Path: {file_path}\n")
                
                if info.get('error'):
                    f.write(f"    Error Message: {info['error']}\n")
                f.write("\n")
        
        # Phase 2 Hospitals with Warnings/Errors (completed but with issues)
        warned_phase2 = {hid: info for hid, info in phase2_summary['hospitals'].items() 
                        if info['status'] == 'completed' and (info.get('warnings_count', 0) > 0 or info.get('errors_count', 0) > 0)}
        if warned_phase2:
            f.write("⚠️  PHASE 2 HOSPITALS WITH WARNINGS/ERRORS:\n")
            for hospital_id, info in sorted(warned_phase2.items(), key=lambda x: x[1]['name']):
                f.write(f"  - {info['name']} ({hospital_id})\n")
                f.write(f"    Format: {info.get('format_type', 'unknown')}\n")
                f.write(f"    Rows: {info.get('total_rows', 0):,}, Columns: {info.get('total_columns', 0)}\n")
                
                if info.get('warnings'):
                    f.write(f"    Warnings ({len(info['warnings'])}):\n")
                    for warning in info['warnings'][:5]:  # Show first 5 warnings
                        f.write(f"      - {warning}\n")
                    if len(info['warnings']) > 5:
                        f.write(f"      ... and {len(info['warnings']) - 5} more warnings\n")
                
                if info.get('errors'):
                    f.write(f"    Errors ({len(info['errors'])}):\n")
                    for error in info['errors'][:5]:  # Show first 5 errors
                        f.write(f"      - {error}\n")
                    if len(info['errors']) > 5:
                        f.write(f"      ... and {len(info['errors']) - 5} more errors\n")
                f.write("\n")
        
        # Phase 2 Missing Hospitals (in downloads but not in analysis)
        if DOWNLOAD_MANIFEST.exists():
            with open(DOWNLOAD_MANIFEST, 'r') as df:
                download_manifest = json.load(df)
                completed_downloads = {
                    k: v for k, v in download_manifest.get("downloads", {}).items()
                    if v.get("status") == "completed"
                }
                
                analyzed_ids = set(phase2_summary['hospitals'].keys())
                downloaded_ids = set(completed_downloads.keys())
                missing_ids = downloaded_ids - analyzed_ids
                
                if missing_ids:
                    f.write("⚠️  HOSPITALS IN DOWNLOADS BUT NOT ANALYZED:\n")
                    for hospital_id in sorted(missing_ids):
                        hospital_info = completed_downloads.get(hospital_id, {})
                        name = hospital_info.get('name', 'Unknown')
                        file_path = hospital_info.get('file_path', 'N/A')
                        file_type = hospital_info.get('file_type', 'unknown')
                        f.write(f"  - {name} ({hospital_id})\n")
                        f.write(f"    File: {file_path}\n")
                        f.write(f"    Type: {file_type}\n")
                        f.write(f"    Debug: Check if file exists and is readable\n")
                    f.write("\n")
        
        f.write("\n" + "=" * 80 + "\n\n")
        
        # Phase 3 Summary
        f.write("PHASE 3: CONFIG GENERATION\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total Analyses Available: {phase3_summary['total_analyses']}\n")
        f.write(f"✅ Completed: {phase3_summary['completed']}\n")
        f.write(f"⏭️  Skipped: {phase3_summary['skipped']}\n")
        f.write(f"❌ Failed: {phase3_summary['failed']}\n")
        f.write(f"Missing: {phase3_summary['total_analyses'] - phase3_summary['completed'] - phase3_summary['skipped'] - phase3_summary['failed']}\n\n")
        
        # Phase 3 Failed Hospitals
        failed_phase3 = {hid: info for hid, info in phase3_summary['hospitals'].items() 
                        if info['status'] == 'failed'}
        if failed_phase3:
            f.write("❌ PHASE 3 FAILED HOSPITALS:\n")
            for hospital_id, info in sorted(failed_phase3.items(), key=lambda x: x[1]['name']):
                f.write(f"  - {info['name']} ({hospital_id})\n")
                f.write(f"    Status: {info['status']}\n")
                f.write(f"    Format Type: {info.get('format_type', 'unknown')}\n")
                
                # Get profile file path from analysis manifest
                if ANALYSIS_MANIFEST.exists():
                    with open(ANALYSIS_MANIFEST, 'r') as af:
                        analysis_manifest = json.load(af)
                        analysis_info = analysis_manifest.get("analyses", {}).get(hospital_id, {})
                        profile_file = analysis_info.get('profile_file', 'N/A')
                        f.write(f"    Profile File: {profile_file}\n")
                
                if info.get('error'):
                    f.write(f"    Error Message: {info['error']}\n")
                    # Try to provide more context about the error
                    error_msg = info['error'].lower()
                    if 'ollama' in error_msg or 'connection' in error_msg:
                        f.write(f"    Debug Hint: Check if Ollama is running (ollama serve)\n")
                    elif 'json' in error_msg or 'parse' in error_msg:
                        f.write(f"    Debug Hint: AI response parsing issue - may need to retry\n")
                    elif 'validation' in error_msg:
                        f.write(f"    Debug Hint: Config validation failed - check profile for data issues\n")
                f.write("\n")
        
        # Phase 3 Missing Hospitals (in analysis but not in configs)
        if ANALYSIS_MANIFEST.exists():
            with open(ANALYSIS_MANIFEST, 'r') as af:
                analysis_manifest = json.load(af)
                completed_analyses = {
                    k: v for k, v in analysis_manifest.get("analyses", {}).items()
                    if v.get("status") == "completed"
                }
                
                config_ids = set(phase3_summary['hospitals'].keys())
                analyzed_ids = set(completed_analyses.keys())
                missing_ids = analyzed_ids - config_ids
                
                if missing_ids:
                    f.write("⚠️  HOSPITALS ANALYZED BUT NOT CONFIGURED:\n")
                    for hospital_id in sorted(missing_ids):
                        hospital_info = completed_analyses.get(hospital_id, {})
                        name = hospital_info.get('name', 'Unknown')
                        profile_file = hospital_info.get('profile_file', 'N/A')
                        format_type = hospital_info.get('format_type', 'unknown')
                        warnings = hospital_info.get('warnings', 0)
                        errors = hospital_info.get('errors', 0)
                        f.write(f"  - {name} ({hospital_id})\n")
                        f.write(f"    Profile: {profile_file}\n")
                        f.write(f"    Format: {format_type}\n")
                        if warnings > 0 or errors > 0:
                            f.write(f"    Issues: {warnings} warnings, {errors} errors\n")
                        f.write(f"    Debug: Profile exists but config generation failed or was skipped\n")
                    f.write("\n")
        
        f.write("\n" + "=" * 80 + "\n\n")
        
        # Final Summary
        f.write("FINAL SUMMARY\n")
        f.write("-" * 80 + "\n")
        f.write(f"📊 Profiles Generated: {final_stats['profiles']}\n")
        f.write(f"📊 Configs Generated: {final_stats['configs']}\n")
        f.write(f"✅ Validated: {final_stats['validated']}\n")
        f.write(f"❌ Rejected: {final_stats['rejected']}\n")
        f.write(f"⏳ Pending: {final_stats['pending']}\n\n")
        
        f.write("FILE LOCATIONS:\n")
        f.write(f"  Profiles: {PROFILES_DIR}\n")
        f.write(f"  Configs: {CONFIGS_DIR}\n")
        if args.no_server:
            f.write(f"  Preview: {DATA_DIR / 'preview_cards.html'}\n")
        f.write("\n")
        
        f.write("=" * 80 + "\n")
        
        # Debugging Tips Section
        f.write("\nDEBUGGING TIPS\n")
        f.write("-" * 80 + "\n")
        f.write("1. For Phase 2 failures:\n")
        f.write("   - Check if download file exists and is readable\n")
        f.write("   - Verify file format (CSV/JSON/ZIP)\n")
        f.write("   - Check file encoding (may need to specify in analyze_csv.py)\n")
        f.write("   - Review profile file for detailed warnings/errors\n\n")
        f.write("2. For Phase 3 failures:\n")
        f.write("   - Ensure Ollama is running: ollama serve\n")
        f.write("   - Check profile file for data quality issues\n")
        f.write("   - Review AI-generated config for validation errors\n")
        f.write("   - Retry failed hospitals individually\n\n")
        f.write("3. For missing hospitals:\n")
        f.write("   - Verify download completed successfully\n")
        f.write("   - Check download_manifest.json for file paths\n")
        f.write("   - Ensure file is in expected location\n")
        f.write("   - Check analysis_manifest.json for analysis status\n\n")
        f.write("4. To retry specific hospitals:\n")
        f.write("   - Delete their entry from analysis_manifest.json (Phase 2)\n")
        f.write("   - Delete their entry from config_manifest.json (Phase 3)\n")
        f.write("   - Re-run the pipeline (it will resume from missing entries)\n\n")
        f.write("=" * 80 + "\n")
    
    return log_file


def main():
    """Main pipeline orchestrator."""
    parser = argparse.ArgumentParser(
        description="Run the complete AI Surveyor pipeline (Phases 2, 3, 4)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline (resumes from existing progress)
  python3 scripts/surveyor/run_full_pipeline.py
  
  # Fresh start (delete all profiles and configs)
  python3 scripts/surveyor/run_full_pipeline.py --fresh
  
  # Generate preview HTML but don't start server
  python3 scripts/surveyor/run_full_pipeline.py --no-server
        """
    )
    
    parser.add_argument(
        '--fresh',
        action='store_true',
        help='Delete existing profiles and configs before running (fresh start)'
    )
    
    parser.add_argument(
        '--no-server',
        action='store_true',
        help='Generate preview HTML but do not start the validation server'
    )
    
    args = parser.parse_args()
    
    # Print welcome message
    print_header("AI SURVEYOR - FULL PIPELINE")
    print(f"Starting at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if args.fresh:
        print("\n⚠️  FRESH START MODE: All existing profiles and configs will be deleted")
        response = input("Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Aborted.")
            return
    
    # Store start time for log
    start_time = datetime.now()
    
    # Phase 2: Analyze CSV files
    if args.fresh:
        delete_profiles(fresh=True)
    
    success_phase2 = run_phase(ANALYZE_SCRIPT, "ANALYZE CSV FILES", 2)
    if not success_phase2:
        print("\n❌ Pipeline stopped: Phase 2 failed")
        # Still write log even on failure
        phase2_summary = get_phase2_summary()
        phase3_summary = get_phase3_summary()
        final_stats = get_pipeline_stats()
        log_file = write_summary_log(start_time, phase2_summary, phase3_summary, final_stats, args)
        print(f"\n📄 Summary log saved to: {log_file}")
        return
    
    # Get Phase 2 summary
    phase2_summary = get_phase2_summary()
    print(f"\n  📊 Phase 2 Results: {phase2_summary['completed']} completed, "
          f"{phase2_summary['failed']} failed, {phase2_summary['skipped']} skipped")
    
    # Phase 3: Generate configs
    if args.fresh:
        delete_configs(fresh=True)
    
    success_phase3 = run_phase(GENERATE_SCRIPT, "GENERATE AI CONFIGS", 3)
    if not success_phase3:
        print("\n❌ Pipeline stopped: Phase 3 failed")
        # Still write log even on failure
        phase3_summary = get_phase3_summary()
        final_stats = get_pipeline_stats()
        log_file = write_summary_log(start_time, phase2_summary, phase3_summary, final_stats, args)
        print(f"\n📄 Summary log saved to: {log_file}")
        return
    
    # Get Phase 3 summary
    phase3_summary = get_phase3_summary()
    print(f"\n  📊 Phase 3 Results: {phase3_summary['completed']} completed, "
          f"{phase3_summary['failed']} failed, {phase3_summary['skipped']} skipped")
    
    # Get final stats BEFORE Phase 4 (so we can write log even if Phase 4 fails)
    final_stats = get_pipeline_stats()
    
    # Phase 4: Preview cards
    if args.no_server:
        # Just generate HTML, don't start server
        success_phase4 = generate_preview_html_only()
        if not success_phase4:
            print("\n❌ Pipeline stopped: Phase 4 failed")
            # Still write log even if Phase 4 fails
            log_file = write_summary_log(start_time, phase2_summary, phase3_summary, final_stats, args)
            print(f"\n📄 Summary log saved to: {log_file}")
            return
        # If Phase 4 succeeded, log will be written in final summary below
    else:
        # For server mode: Generate HTML first, then write log, then start server
        # This ensures log is written even if server fails to start
        print_section("PHASE 4: PREVIEW CARDS & VALIDATION SERVER")
        
        # Import preview_cards functions directly to generate HTML
        sys.path.insert(0, str(SCRIPT_DIR))
        from preview_cards import load_config_manifest, generate_html
        
        try:
            manifest = load_config_manifest()
            print("  📊 Generating preview cards HTML...")
            
            html_content = generate_html(manifest)
            
            preview_file = DATA_DIR / "preview_cards.html"
            with open(preview_file, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            print(f"  ✅ Preview HTML saved to: {preview_file}")
            
            # Write log BEFORE starting server (so it's saved even if server fails)
            log_file = write_summary_log(start_time, phase2_summary, phase3_summary, final_stats, args)
            print(f"  📄 Summary log saved to: {log_file}")
            
            # Now start the server
            print(f"\n  🌐 Starting validation server on http://localhost:8765")
            print("     Open this URL in your browser to review configs")
            print("     Press Ctrl+C to stop the server\n")
            
            from preview_cards import ValidationHandler, HTTPServer, SERVER_PORT
            server = HTTPServer(('localhost', SERVER_PORT), ValidationHandler)
            server.serve_forever()
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Server stopped by user")
            # Log was already written, so we're good
        except Exception as e:
            print(f"\n  ❌ Failed to start server: {e}")
            # Log was already written, so we're good
            return
    
    # Final summary (only reached if --no-server or if server was interrupted)
    print_header("PIPELINE COMPLETE")
    
    print(f"  📊 Profiles generated: {final_stats['profiles']}")
    print(f"  📊 Configs generated: {final_stats['configs']}")
    print(f"  ✅ Validated: {final_stats['validated']}")
    print(f"  ❌ Rejected: {final_stats['rejected']}")
    print(f"  ⏳ Pending: {final_stats['pending']}")
    
    print(f"\n  📁 Profiles: {PROFILES_DIR}")
    print(f"  📁 Configs: {CONFIGS_DIR}")
    
    if args.no_server:
        preview_file = DATA_DIR / "preview_cards.html"
        print(f"  📁 Preview: {preview_file}")
        print(f"\n  💡 To view the preview, run: python3 {PREVIEW_SCRIPT}")
        # Write log for --no-server case
        log_file = write_summary_log(start_time, phase2_summary, phase3_summary, final_stats, args)
        print(f"\n  📄 Detailed summary log saved to: {log_file}")
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        print("Progress has been saved. You can resume by running the script again.")
        sys.exit(1)
