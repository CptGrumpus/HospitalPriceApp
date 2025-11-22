import requests
import os
import sys
import zipfile
import io

def inspect_url(url, output_path="data/samples/preview.txt"):
    print(f"--- Inspecting: {url} ---")
    
    try:
        # stream=True is critical so we don't download the whole file
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        with requests.get(url, stream=True, headers=headers) as r:
            r.raise_for_status()
            
            # 1. Check Size
            size_bytes = r.headers.get('content-length')
            if size_bytes:
                size_mb = int(size_bytes) / (1024 * 1024)
                print(f"Total File Size: {size_mb:.2f} MB")
            else:
                print("Total File Size: Unknown (Server didn't send content-length)")

            # 2. Peek at Content Type
            content_type = r.headers.get('content-type', 'unknown')
            print(f"Content Type: {content_type}")

            # 3. Handle ZIP files vs Regular files
            # Check if 'zip' is in the content-type or if '.zip' appears in the URL path (ignoring query params)
            if 'zip' in content_type or '.zip' in url.split('?')[0].lower():
                print("\n--- Detected ZIP file ---")
                handle_zip_stream(r, output_path)
            else:
                print("\n--- Detected Text/JSON file ---")
                handle_text_stream(r, output_path)

    except Exception as e:
        print(f"Error: {e}")

def handle_text_stream(response, output_path):
    print(f"Downloading first 100 lines to {output_path} ...")
    with open(output_path, 'wb') as f:
        lines_captured = 0
        for line in response.iter_lines():
            if line: 
                f.write(line + b'\n')
                lines_captured += 1
            
            if lines_captured >= 100:
                break
    print("Done. Check the preview file.")

def handle_zip_stream(response, output_path):
    # ZIPs are tricky to stream because the Central Directory is at the END of the file.
    # However, we can try to download a chunk and see if we get the first local file header.
    # If the file is small (< 50MB), just download the whole thing.
    
    temp_zip = "data/samples/temp_partial.zip"
    print(f"Downloading ZIP to {temp_zip}...")
    
    with open(temp_zip, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print("Attempting to read zip...")
    try:
        # We rely on the zipfile library's ability to handle some truncated files 
        # or we just list what we can. *Warning*: Valid ZIPs require the footer. 
        # This is a 'best effort' hack. If it fails, we might need to download more.
        # A robust production way is to use HTTP Range headers to get the END of the file first.
        
        # actually, python's zipfile is strict. Let's try a different approach:
        # If we can't open it, we just say "It's a zip, download full file to inspect".
        # But let's try:
        if zipfile.is_zipfile(temp_zip):
            with zipfile.ZipFile(temp_zip, 'r') as z:
                print(f"Files inside: {z.namelist()}")
                first_file = z.namelist()[0]
                print(f"Reading first 100 lines of: {first_file}")
                with z.open(first_file) as zf, open(output_path, 'wb') as out:
                    for i in range(100):
                        out.write(zf.readline())
            print("Success! Extracted sample from zip.")
        else:
            print("Could not open partial zip (headers missing or file too truncated).")
            print("Recommendation: For ZIPs, we might need to download the whole thing or use Range headers.")
            
    except Exception as e:
        print(f"Zip Error (expected for partial download): {e}")
        print("NOTE: To properly inspect a ZIP without full download, we need HTTP Range support.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/inspect_file.py <URL>")
        sys.exit(1)
    
    url = sys.argv[1]
    inspect_url(url)

