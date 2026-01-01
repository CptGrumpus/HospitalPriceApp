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


def get_sample_data(hospital_id, config, max_rows=5):
    """
    Load sample data from the hospital's file.
    Returns list of dicts with sample rows.
    """
    hospital_dir = DOWNLOADS_DIR / hospital_id
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
            header_row = config.get('header_row', 0)
            encoding = config.get('encoding', 'utf-8')
            
            try:
                df = pd.read_csv(data_file, header=header_row, nrows=max_rows, 
                                dtype=str, encoding=encoding)
            except:
                df = pd.read_csv(data_file, header=header_row, nrows=max_rows, 
                                dtype=str, encoding='iso-8859-1')
            
            return df.to_dict('records'), None
            
        elif data_file.suffix.lower() == '.json':
            with open(data_file, 'r') as f:
                data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(data, list):
                return data[:max_rows], None
            elif isinstance(data, dict):
                # Look for common array keys
                for key in ['standard_charge_information', 'data', 'items', 'records']:
                    if key in data and isinstance(data[key], list):
                        return data[key][:max_rows], None
                return [data], None
            
    except Exception as e:
        return None, str(e)
    
    return None, "Unknown error"


def slugify(name):
    """Convert hospital name to a clean slug."""
    import re
    # Remove special characters, replace spaces with underscores
    slug = re.sub(r'[^\w\s-]', '', name)
    slug = re.sub(r'[-\s]+', '_', slug)
    return slug.upper()[:50]


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
    
    for hospital_id, info in sorted_configs:
        if info.get("status") != "completed":
            continue
            
        name = info.get("name", "Unknown")
        config = load_config(hospital_id)
        profile = load_profile(hospital_id)
        
        if not config:
            continue
        
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
        
        # Sample data
        sample_data, sample_error = get_sample_data(hospital_id, config)
        
        sample_html = ""
        if sample_error:
            sample_html = f'<p class="error">Error loading sample: {html.escape(sample_error)}</p>'
        elif sample_data:
            # Create a simple table
            sample_html = '<div class="sample-table-wrapper"><table class="sample-table"><thead><tr>'
            
            # Get columns from first row
            if sample_data:
                cols = list(sample_data[0].keys())[:10]  # Limit to 10 columns
                for col in cols:
                    sample_html += f'<th>{html.escape(str(col)[:30])}</th>'
                sample_html += '</tr></thead><tbody>'
                
                for row in sample_data[:5]:
                    sample_html += '<tr>'
                    for col in cols:
                        val = str(row.get(col, ''))[:50]
                        sample_html += f'<td>{html.escape(val)}</td>'
                    sample_html += '</tr>'
                
                sample_html += '</tbody></table></div>'
        else:
            sample_html = '<p class="muted">No sample data available</p>'
        
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
        cards_html.append(card_html)
    
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
    
    # Start server
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

