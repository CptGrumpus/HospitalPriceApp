# Hospital Price Search Application

## Overview
This is a Hospital Price Search web application that allows users to search for medical procedures and compare prices across different hospitals and insurance payers. The application uses FastAPI for the backend and vanilla JavaScript for the frontend.

**Current State**: Fully configured and running in the Replit environment with all dependencies installed.

**Last Updated**: November 27, 2025

## Project Architecture

### Tech Stack
- **Backend**: FastAPI (Python)
- **Frontend**: HTML/JavaScript (vanilla JS)
- **Database**: SQLite (local file-based)
- **Web Server**: Uvicorn (ASGI server)

### Directory Structure
```
.
├── src/
│   ├── static/
│   │   └── index.html          # Frontend web interface
│   ├── main.py                 # FastAPI application and API endpoints
│   └── database.py             # SQLAlchemy models and database setup
├── scripts/
│   ├── ingest_all.py           # Data ingestion script (all sources)
│   ├── ingest_hcpcs_definitions.py  # HCPCS code definitions
│   ├── ingest_tall.py          # Tall format data ingestion
│   ├── ingest_wide.py          # Wide format data ingestion
│   └── inspect_file.py         # File inspection utility
├── run.py                      # Application entry point (dev server)
├── requirements.txt            # Python dependencies
├── hospital.db                 # SQLite database (created on first run)
└── main.js                     # Minified JS (legacy, not used)
```

### Database Schema
The application uses three main tables:
1. **items**: Medical procedures/services with codes, descriptions, hospital IDs
2. **prices**: Price information for each item by payer/plan
3. **code_definitions**: Official definitions for medical codes (CPT, HCPCS, DRG)

## Development Setup

### Dependencies
All Python dependencies are in `requirements.txt`:
- fastapi - Web framework
- uvicorn - ASGI server
- sqlalchemy - ORM for database operations
- pandas - Data processing (for ingestion scripts)
- requests - HTTP library (for data fetching)

### Running Locally
The application is configured to run on port 5000 with the following settings:
- Host: `0.0.0.0` (allows external access)
- Port: `5000` (Replit's webview port)
- Proxy headers: Enabled (for Replit's proxy environment)

The workflow "FastAPI Server" runs: `python run.py`

### Database Initialization
The database is automatically created when you run `python src/database.py`. The tables are:
- `items`: Core medical items/procedures
- `prices`: Associated pricing data
- `code_definitions`: Official medical code definitions

To populate the database with actual data, use the ingestion scripts in the `scripts/` directory.

## API Endpoints

### GET `/`
Returns the main HTML interface.

### GET `/search?q={query}`
Search for medical procedures by code or description.

**Parameters**:
- `q` (required): Search query string

**Response**:
```json
{
  "count": 2,
  "results": [
    {
      "hospital_id": "UOFM",
      "code": "99213",
      "code_type": "CPT",
      "description": "Office Visit Level 3",
      "definition": "Office outpatient visit...",
      "setting": "outpatient",
      "prices": [
        {
          "payer": "DISCOUNTED_CASH",
          "plan": null,
          "amount": 150.00,
          "notes": null
        }
      ]
    }
  ]
}
```

## Deployment

The application is configured for autoscale deployment on Replit with:
- **Deployment Target**: Autoscale (stateless web application)
- **Run Command**: `uvicorn src.main:app --host 0.0.0.0 --port 5000 --forwarded-allow-ips * --proxy-headers`

The database file (`hospital.db`) will persist between deployments since it's in the project directory.

## Recent Changes
- **2025-11-27**: Initial Replit environment setup
  - Installed Python 3.11 and all dependencies
  - Created `run.py` entry point with proper host/port configuration
  - Configured FastAPI to trust proxy headers (required for Replit)
  - Initialized database schema
  - Set up workflow for development server
  - Configured autoscale deployment

## Data Ingestion

The `scripts/` directory contains data ingestion tools:
- `ingest_all.py`: Main script to ingest all data sources
- `ingest_tall.py`: For tall-format pricing data
- `ingest_wide.py`: For wide-format pricing data  
- `ingest_hcpcs_definitions.py`: For HCPCS code definitions

Run these scripts to populate the database with actual hospital pricing data.

## Notes
- The SQLite database is local to the Replit instance
- For production with multiple instances, consider migrating to PostgreSQL
- The frontend uses inline JavaScript for simplicity (no build step required)
- The application searches up to 10,000 results per query
