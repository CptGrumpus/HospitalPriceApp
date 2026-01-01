#!/usr/bin/env python3
"""
Phase 1: Download Manager for Hospital Pricing Files

Downloads all hospital pricing files from michigan_hospitals_raw.json
Supports: CSV, JSON, ZIP files
Features:
- Resumable (tracks progress in manifest)
- Extracts ZIP files automatically
- Organizes files by hospital
- Uses direct hospital URLs with proper headers
"""

import json
import os
import sys
import time
import zipfile
import requests
from pathlib import Path
from datetime import datetime

# Configuration
DATA_DIR = Path(__file__).parent.parent.parent / "data"
DOWNLOADS_DIR = DATA_DIR / "downloads"
MANIFEST_FILE = DOWNLOADS_DIR / "download_manifest.json"
HOSPITALS_JSON = DATA_DIR / "michigan_hospitals_raw.json"

# HTTP Headers to avoid being blocked
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/csv,application/json,application/zip,*/*',
    'Accept-Language': 'en-US,en;q=0.9',
}

# File type priorities (lower = better)
# We prefer CSV files, then JSON, then ZIP
FILE_PRIORITY = {
    ("csv", "spreadsheet"): 1,    # Best: Original CSV spreadsheet
    ("csv", "converted"): 2,      # Good: Pre-converted CSV
    ("csv", "other"): 3,          # OK: Other CSV
    ("json", "other"): 4,         # OK: JSON format
    ("json", "converted"): 5,     # OK: Converted JSON
    ("zip", "unknown"): 6,        # Acceptable: ZIP (will extract)
    ("json", "spreadsheet"): 7,   # Rare
}

def load_manifest():
    """Load or create download manifest."""
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE, 'r') as f:
            return json.load(f)
    return {
        "created": datetime.now().isoformat(),
        "last_updated": None,
        "downloads": {},
        "stats": {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0
        }
    }

def save_manifest(manifest):
    """Save manifest to disk."""
    manifest["last_updated"] = datetime.now().isoformat()
    with open(MANIFEST_FILE, 'w') as f:
        json.dump(manifest, f, indent=2)

def get_best_file(hospital):
    """
    Select the best file to download for a hospital.
    Prioritizes CSV files with valid URLs.
    """
    files = hospital.get("files", [])
    if not files:
        return None
    
    # Score each file
    scored_files = []
    for f in files:
        suffix = f.get("filesuffix", "").lower()
        ftype = f.get("filetype", "").lower()
        url = f.get("url", "")
        
        # Must have a URL
        if not url:
            continue
        
        # Skip if URL doesn't look like a data file
        if not any(url.lower().endswith(ext) for ext in ['.csv', '.json', '.zip', '.xlsx', '.xls']):
            # Some URLs don't have extensions, still try them
            pass
            
        priority = FILE_PRIORITY.get((suffix, ftype), 99)
        scored_files.append((priority, f))
    
    if not scored_files:
        return None
    
    # Sort by priority (lowest first)
    scored_files.sort(key=lambda x: x[0])
    return scored_files[0][1]

def sanitize_filename(name):
    """Create a safe directory/file name."""
    # Remove special characters, replace spaces with underscores
    safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
    return safe[:100]  # Limit length

def download_file(url, dest_path, timeout=300):
    """
    Download a file with progress tracking.
    Returns (success, error_message, actual_size)
    """
    try:
        print(f"    Downloading: {url[:80]}...")
        
        response = requests.get(url, stream=True, timeout=timeout, headers=HEADERS, allow_redirects=True)
        response.raise_for_status()
        
        # Check if we got HTML instead of data (some sites return error pages with 200)
        content_type = response.headers.get('content-type', '').lower()
        if 'text/html' in content_type and not url.endswith('.html'):
            return False, f"Got HTML page instead of data file (content-type: {content_type})", 0
        
        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Progress indicator
                    if total_size > 0:
                        pct = (downloaded / total_size) * 100
                        if downloaded % (1024 * 1024) < 8192:  # Every ~1MB
                            print(f"    Progress: {pct:.1f}% ({downloaded // (1024*1024)}MB)", end='\r')
        
        print(f"    Downloaded: {dest_path.name} ({downloaded // 1024}KB)                    ")
        return True, None, downloaded
        
    except requests.exceptions.Timeout:
        return False, "Timeout (file may be too large)", 0
    except requests.exceptions.HTTPError as e:
        return False, f"HTTP Error: {e.response.status_code}", 0
    except requests.exceptions.RequestException as e:
        return False, str(e), 0
    except Exception as e:
        return False, str(e), 0

def extract_zip(zip_path, extract_dir):
    """
    Extract a ZIP file and return list of extracted files.
    """
    extracted = []
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Filter for data files only
            for name in zf.namelist():
                if name.endswith(('.csv', '.json', '.xlsx', '.xls')):
                    zf.extract(name, extract_dir)
                    extracted.append(extract_dir / name)
                    print(f"    Extracted: {name}")
        return extracted, None
    except Exception as e:
        return [], str(e)

def process_hospital(hospital, manifest):
    """
    Process a single hospital: download best file.
    Returns status string.
    """
    hospital_id = hospital.get("id", "unknown")
    hospital_name = hospital.get("name", "Unknown Hospital")
    safe_name = sanitize_filename(hospital_name)
    
    # Check if already processed
    if hospital_id in manifest["downloads"]:
        status = manifest["downloads"][hospital_id].get("status")
        if status == "completed":
            return "skipped"
    
    print(f"\n{'='*60}")
    print(f"Hospital: {hospital_name}")
    print(f"ID: {hospital_id}")
    
    # Select best file
    best_file = get_best_file(hospital)
    if not best_file:
        print("  ⚠️  No downloadable files found")
        manifest["downloads"][hospital_id] = {
            "name": hospital_name,
            "status": "no_files",
            "timestamp": datetime.now().isoformat()
        }
        return "no_files"
    
    file_suffix = best_file.get("filesuffix", "csv")
    file_type = best_file.get("filetype", "unknown")
    filename = best_file.get("filename", "unknown")
    
    # Use direct URL from hospital website
    download_url = best_file.get("url", "")
    
    if not download_url:
        print("  ⚠️  No URL available for this file")
        manifest["downloads"][hospital_id] = {
            "name": hospital_name,
            "status": "no_url",
            "timestamp": datetime.now().isoformat()
        }
        return "no_files"
    
    print(f"  Selected: {filename} ({file_suffix}, {file_type})")
    print(f"  URL: {download_url[:70]}...")
    
    # Determine actual file extension from URL (might differ from filesuffix)
    url_ext = download_url.split('.')[-1].lower().split('?')[0]
    if url_ext in ['csv', 'json', 'zip', 'xlsx', 'xls']:
        actual_ext = url_ext
    else:
        actual_ext = file_suffix
    
    # Determine destination
    hospital_dir = DOWNLOADS_DIR / safe_name
    dest_filename = f"{safe_name}.{actual_ext}"
    dest_path = hospital_dir / dest_filename
    
    # Download
    success, error, file_size = download_file(download_url, dest_path)
    
    if not success:
        print(f"  ❌ Download failed: {error}")
        manifest["downloads"][hospital_id] = {
            "name": hospital_name,
            "status": "failed",
            "error": error,
            "url": download_url,
            "timestamp": datetime.now().isoformat()
        }
        return "failed"
    
    # Handle ZIP files
    extracted_files = []
    if actual_ext == "zip":
        print("  📦 Extracting ZIP...")
        extracted_files, extract_error = extract_zip(dest_path, hospital_dir)
        if extract_error:
            print(f"  ⚠️  Extraction warning: {extract_error}")
    
    # Update manifest
    manifest["downloads"][hospital_id] = {
        "name": hospital_name,
        "status": "completed",
        "file_path": str(dest_path),
        "file_type": actual_ext,
        "file_size": file_size,
        "extracted_files": [str(f) for f in extracted_files],
        "source_url": download_url,
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"  ✅ Success!")
    return "completed"

def main():
    """Main download orchestrator."""
    print("=" * 60)
    print("  HOSPITAL FILE DOWNLOAD MANAGER - Phase 1")
    print("=" * 60)
    
    # Load hospitals
    if not HOSPITALS_JSON.exists():
        print(f"ERROR: {HOSPITALS_JSON} not found!")
        sys.exit(1)
    
    with open(HOSPITALS_JSON, 'r') as f:
        hospitals = json.load(f)
    
    print(f"Found {len(hospitals)} hospitals in manifest")
    
    # Create downloads directory
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load/create manifest
    manifest = load_manifest()
    manifest["stats"]["total"] = len(hospitals)
    
    # Count existing
    already_done = sum(1 for h in hospitals 
                       if manifest["downloads"].get(h.get("id", ""), {}).get("status") == "completed")
    print(f"Already downloaded: {already_done}")
    print(f"Remaining: {len(hospitals) - already_done}")
    
    # Process each hospital
    stats = {"completed": 0, "failed": 0, "skipped": 0, "no_files": 0}
    
    try:
        for i, hospital in enumerate(hospitals):
            print(f"\n[{i+1}/{len(hospitals)}]", end="")
            
            status = process_hospital(hospital, manifest)
            stats[status] = stats.get(status, 0) + 1
            
            # Save manifest periodically
            if (i + 1) % 5 == 0:
                manifest["stats"] = stats
                save_manifest(manifest)
                
            # Small delay to be nice to the server
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Progress saved.")
    
    # Final save
    manifest["stats"] = stats
    save_manifest(manifest)
    
    # Summary
    print("\n" + "=" * 60)
    print("  DOWNLOAD SUMMARY")
    print("=" * 60)
    print(f"  Total Hospitals: {len(hospitals)}")
    print(f"  ✅ Completed: {stats.get('completed', 0)}")
    print(f"  ⏭️  Skipped (already done): {stats.get('skipped', 0)}")
    print(f"  ❌ Failed: {stats.get('failed', 0)}")
    print(f"  ⚠️  No files: {stats.get('no_files', 0)}")
    print(f"\n  Manifest saved to: {MANIFEST_FILE}")
    print("=" * 60)

if __name__ == "__main__":
    main()

