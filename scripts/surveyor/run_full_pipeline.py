#!/usr/bin/env python3
"""
Master Pipeline Script - Runs Phases 2, 3, and 4 in Sequence

This script orchestrates the entire AI Surveyor pipeline:
- Phase 2: analyze_csv.py - Analyzes downloaded files and generates profiles
- Phase 3: generate_config.py - Uses AI to generate ingestion configs
- Phase 4: preview_cards.py - Generates preview cards for validation

Usage:
    python3 scripts/surveyor/run_full_pipeline.py [--fresh] [--no-server]

Options:
    --fresh      Delete existing profiles and configs before running (fresh start)
    --no-server  Generate preview HTML but don't start the validation server
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
ANALYSIS_MANIFEST = PROFILES_DIR / "analysis_manifest.json"
CONFIG_MANIFEST = CONFIGS_DIR / "config_manifest.json"

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
    
    # Phase 2: Analyze CSV files
    if args.fresh:
        delete_profiles(fresh=True)
    
    success_phase2 = run_phase(ANALYZE_SCRIPT, "ANALYZE CSV FILES", 2)
    if not success_phase2:
        print("\n❌ Pipeline stopped: Phase 2 failed")
        return
    
    # Phase 3: Generate configs
    if args.fresh:
        delete_configs(fresh=True)
    
    success_phase3 = run_phase(GENERATE_SCRIPT, "GENERATE AI CONFIGS", 3)
    if not success_phase3:
        print("\n❌ Pipeline stopped: Phase 3 failed")
        return
    
    # Phase 4: Preview cards
    if args.no_server:
        # Just generate HTML, don't start server
        success_phase4 = generate_preview_html_only()
        if not success_phase4:
            print("\n❌ Pipeline stopped: Phase 4 failed")
            return
    else:
        # Run full preview script (generates HTML and starts server)
        success_phase4 = run_phase(PREVIEW_SCRIPT, "PREVIEW CARDS & VALIDATION SERVER", 4)
        if not success_phase4:
            print("\n❌ Pipeline stopped: Phase 4 failed")
            return
    
    # Final summary
    print_header("PIPELINE COMPLETE")
    
    stats = get_pipeline_stats()
    print(f"  📊 Profiles generated: {stats['profiles']}")
    print(f"  📊 Configs generated: {stats['configs']}")
    print(f"  ✅ Validated: {stats['validated']}")
    print(f"  ❌ Rejected: {stats['rejected']}")
    print(f"  ⏳ Pending: {stats['pending']}")
    
    print(f"\n  📁 Profiles: {PROFILES_DIR}")
    print(f"  📁 Configs: {CONFIGS_DIR}")
    
    if args.no_server:
        preview_file = DATA_DIR / "preview_cards.html"
        print(f"  📁 Preview: {preview_file}")
        print(f"\n  💡 To view the preview, run: python3 {PREVIEW_SCRIPT}")
    else:
        print(f"\n  🌐 Preview server should be running at: http://localhost:8765")
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        print("Progress has been saved. You can resume by running the script again.")
        sys.exit(1)
