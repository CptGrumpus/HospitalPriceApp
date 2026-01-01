# Hospital Price App - TODO List

## Project Status
**Last Updated:** December 30, 2025

### AI Surveyor Pipeline Progress
| Phase | Status | Description |
|-------|--------|-------------|
| Phase 1 | ✅ Complete | Download Manager (CSV, JSON, ZIP support) |
| Phase 1b | ✅ Complete | Download 167 Michigan hospitals (128 succeeded) |
| Phase 2 | ✅ Complete | Deep CSV Analyzer - 128 profiles generated |
| Phase 3 | ✅ Ready | AI Config Generator - Run when ready |
| Phase 4 | ⏳ Pending | Preview Card Generator (human validation) |
| Phase 5 | ⏳ Pending | Universal Bulk Ingestor |

---

## Download Results Summary
- **Total Hospitals:** 167
- **✅ Successfully Downloaded:** 126
- **❌ Failed:** 35
- **⚠️ No Files Available:** 4

---

## 🔴 HIGH PRIORITY: Fix Failed Hospital Downloads

### 35 Failed Downloads to Fix Later

These hospitals need manual URL updates. The URLs may have changed since the data was collected.

#### HTTP 403 Errors (Access Forbidden) - 21 hospitals
These sites are blocking automated requests. May need Playwright/browser automation.

| Hospital | Issue |
|----------|-------|
| MyMichigan Medical Center Alma | 403 Forbidden |
| MyMichigan Medical Center Alpena | 403 Forbidden |
| MyMichigan Medical Center Clare | 403 Forbidden |
| MyMichigan Medical Center Gladwin | 403 Forbidden |
| MyMichigan Medical Center Midland | 403 Forbidden |
| MyMichigan Medical Center Saginaw | 403 Forbidden |
| MyMichigan Medical Center Sault | 403 Forbidden |
| MyMichigan Medical Center Standish | 403 Forbidden |
| MyMichigan Medical Center Tawas | 403 Forbidden |
| MyMichigan Medical Center Towne Centre | 403 Forbidden |
| MyMichigan Medical Center West Branch | 403 Forbidden |
| Select Specialty Hospital - Ann Arbor | 403 Forbidden |
| Select Specialty Hospital - Battle Creek | 403 Forbidden |
| Select Specialty Hospital - Flint | 403 Forbidden |
| Select Specialty Hospital - Grosse Pointe | 403 Forbidden |
| Select Specialty Hospital - Macomb County | 403 Forbidden |
| Select Specialty Hospital - Northwest Detroit | 403 Forbidden |
| Select Specialty Hospital - Oakland | 403 Forbidden |
| Select Specialty Hospital - Saginaw | 403 Forbidden |
| Select Specialty Hospital - Spectrum Health | 403 Forbidden |
| Select Specialty Hospitals - Downriver | 403 Forbidden |
| **University of Michigan Health System** | 403 Forbidden |

#### HTTP 404 Errors (Not Found) - 10 hospitals
URLs have changed. Need to find new URLs on hospital websites.

| Hospital | Old URL Domain |
|----------|----------------|
| Beacon Allegan | healthcare.ascension.org |
| Beacon Dowagiac | healthcare.ascension.org |
| Beacon Kalamazoo | healthcare.ascension.org |
| Behavioral Center of Michigan | behavioralcenter.com |
| Deckerville Community Hospital | aspirerhs.org |
| Eaton Rapids Medical Center | quadax.revenuemasters.com |
| Hills & Dales General Hospital | aspirerhs.org |
| Marlette Regional Hospital | aspirerhs.org |
| Schoolcraft Memorial Hospital | quadax.revenuemasters.com |

#### HTTP 401 Errors (Unauthorized) - 2 hospitals
| Hospital | Notes |
|----------|-------|
| Pioneer Specialty Hospital - Garden City | Requires authentication |
| Pioneer Specialty Hospital - Pontiac | Requires authentication |

#### SSL Certificate Error - 1 hospital
| Hospital | Notes |
|----------|-------|
| Hurley Medical Center Main Campus | SSL cert verification failed |

#### Other Errors - 1 hospital
| Hospital | Notes |
|----------|-------|
| Pontiac General Hospital | Returns HTML instead of CSV |

### 4 Hospitals with No Files Available
These hospitals don't have downloadable files in the source data:
- Henry Ford Behavioral Health Hospital
- Munising Memorial Hospital
- Samaritan Center
- Spectrum Health Rehab and Nursing Center - Fuller Avenue

---

## How to Fix Failed Downloads

### Option 1: Manual URL Update
1. Visit the hospital's website
2. Find the "Price Transparency" or "Standard Charges" page
3. Get the direct CSV/JSON download link
4. Update `data/michigan_hospitals_raw.json` with the new URL
5. Re-run the download script

### Option 2: Playwright Browser Automation
For 403 errors (bot detection), create a Playwright script that:
1. Opens the hospital website in a real browser
2. Navigates to the price transparency page
3. Downloads the file like a human would

### Option 3: Skip for Now
Focus on the 126 hospitals that downloaded successfully first.
Come back to fix these later.

---

## Future Improvements

- [ ] Add retry logic with exponential backoff
- [ ] Add Playwright fallback for 403 errors
- [ ] Add SSL verification bypass option (with warning)
- [ ] Create a "hospital URL updater" utility script
- [ ] Set up scheduled re-scraping of hospitalpricingfiles.org for new URLs

---

## Files & Directories

| Path | Description |
|------|-------------|
| `data/downloads/` | Downloaded hospital files |
| `data/downloads/download_manifest.json` | Download status tracking |
| `data/michigan_hospitals_raw.json` | Source hospital list (needs URL updates) |
| `scripts/surveyor/download_all.py` | Download manager script |
| `scripts/surveyor/analyze_csv.py` | CSV analyzer (Phase 2) |
| `scripts/surveyor/generate_config.py` | AI config generator (Phase 3) |

---

## Notes
- The download manifest tracks all attempts - just re-run `download_all.py` to retry failed downloads
- Some hospitals (like U of M) are high-priority and worth extra effort to obtain manually

