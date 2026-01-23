# Hospital Price App - TODO List

## Project Status
**Last Updated:** January 23, 2026

### AI Surveyor Pipeline Progress
| Phase | Status | Description |
|-------|--------|-------------|
| Phase 1 | ✅ Complete | Download Manager (CSV, JSON, ZIP support) |
| Phase 1b | ✅ Complete | Download 167 Michigan hospitals (126 succeeded) |
| Phase 2 | ✅ Complete | Deep CSV Analyzer - profiles generated |
| Phase 3 | ✅ Complete | AI Config Generator |
| Phase 4 | ✅ Complete | Preview Card Generator (126/129 validated) |
| Phase 5 | ✅ Complete | Universal Bulk Ingestor |
| Phase 6 | ✅ Complete | Performance Optimization (FTS5 + Pagination) |

### Backend Features Implemented
| Feature | Status | Notes |
|---------|--------|-------|
| FTS5 Full-Text Search | ✅ Done | 1.5ms search on 8M+ items |
| Pagination API | ✅ Done | 50 results per page |
| Lazy price loading | ✅ Done | Only loads prices for displayed items |
| Database indexes | ✅ Done | Composite indexes for common queries |
| Stats endpoint | ✅ Done | `/stats` for database info |

### Current Database (After Reingestion)
- **Items:** ~8M+ procedures
- **Prices:** ~97M+ price entries
- **Hospitals:** 126 Michigan hospitals
- **Database Size:** ~10GB

---

## 🔴 P0 - Launch Critical (Do Before First Users)

These are essential for a good first impression:

| Feature | Status | Effort | Notes |
|---------|--------|--------|-------|
| Search examples on homepage | [ ] TODO | 1 hour | Show "Try: MRI, Knee replacement, 99213" |
| Hospital count banner | [ ] TODO | 30 min | "Searching 126 Michigan hospitals" |
| Data freshness date | [ ] TODO | 30 min | "Prices updated: January 2026" |
| About/FAQ page | [ ] TODO | 2 hours | Explain why prices vary, how to use |
| Mobile responsive check | [ ] TODO | 2 hours | 50%+ users on mobile |
| Disclaimer footer | [ ] TODO | 30 min | Legal disclaimer about estimates |
| Empty state improvement | [ ] TODO | 1 hour | Show suggestions when no search |

---

## 🟡 P1 - High Impact (First Week After Launch)

| Feature | Status | Effort | Notes |
|---------|--------|--------|-------|
| **Price comparison view** | [ ] TODO | 4 hours | Compare same procedure across hospitals - KILLER FEATURE |
| Insurance/payer filter | [ ] TODO | 2 hours | "Show me Blue Cross prices only" |
| Cash price quick filter | [ ] TODO | 1 hour | Many users are uninsured/cash-pay |
| Shareable URLs | [ ] TODO | 2 hours | `/search?q=MRI` for sharing results |
| "Good deal" indicator | [ ] TODO | 3 hours | Show if price is above/below median |
| No results suggestions | [ ] TODO | 1 hour | Helpful message when search fails |

---

## 🟢 P2 - Nice to Have (Month 1)

| Feature | Status | Effort | Notes |
|---------|--------|--------|-------|
| Popular searches display | [ ] TODO | 2 hours | Show trending procedures |
| Hospital detail pages | [ ] TODO | 4 hours | `/hospital/corewell-beaumont` |
| Recent searches (local) | [ ] TODO | 2 hours | Remember user's searches |
| Search autocomplete | [ ] TODO | 4 hours | Suggest as user types |
| Geographic filtering | [ ] TODO | 4 hours | Filter by city/region |
| Price alerts (email) | [ ] TODO | 8 hours | "Notify when price changes" |

---

## 🔵 P3 - Future Enhancements

### API Endpoints to Add
| Endpoint | Purpose |
|----------|---------|
| `GET /hospitals` | List all hospitals with counts |
| `GET /hospitals/{id}` | Hospital detail with all items |
| `GET /payers` | List all insurance payers |
| `GET /popular` | Most searched procedures |
| `GET /compare?code=99213` | Compare one code across hospitals |

### Database Schema Enhancements
- [ ] **Modifiers Field**: Add `modifiers` field to Item table (CPT modifiers like -25, -59)
- [ ] **Drug Information Fields**: Add `drug_unit` and `drug_type` fields
- [ ] **Revenue Code Field**: Store code|2/code|3 for filtering
- [ ] **Hospital Metadata**: Store city, address, phone from source data

### UI Enhancements (Low Priority)
- [ ] Revenue code filtering (for advanced users)
- [ ] Price distribution histogram
- [ ] Code-payer matrix view
- [ ] Dark mode

### SEO & Marketing
- [ ] Meta tags (title, description, OpenGraph)
- [ ] Sitemap for Google indexing
- [ ] Schema.org markup for medical services
- [ ] Analytics tracking (what people search for)

---

## Download Results Summary
- **Total Hospitals:** 167
- **✅ Successfully Downloaded:** 126
- **❌ Failed:** 35
- **⚠️ No Files Available:** 4

---

## Failed Hospital Downloads (Lower Priority)

### HTTP 403 Errors (Access Forbidden) - 21 hospitals
These sites block automated requests. May need Playwright/browser automation.

| Hospital | Issue |
|----------|-------|
| MyMichigan Medical Center (11 locations) | 403 Forbidden |
| Select Specialty Hospital (10 locations) | 403 Forbidden |
| **University of Michigan Health System** | 403 Forbidden - HIGH PRIORITY |

### HTTP 404 Errors (Not Found) - 10 hospitals
| Hospital | Old URL Domain |
|----------|----------------|
| Beacon Allegan, Dowagiac, Kalamazoo | healthcare.ascension.org |
| Deckerville, Hills & Dales, Marlette | aspirerhs.org |
| Others | Various |

### Other Errors
- 2 hospitals: 401 Unauthorized (requires auth)
- 1 hospital: SSL certificate error
- 1 hospital: Returns HTML instead of CSV
- 4 hospitals: No files available in source data

---

## Code Description Status

### Current Coverage by Code Type
| Code Type | Coverage | Notes |
|-----------|----------|-------|
| **CPT** | 97.3% | ✅ Well covered |
| **HCPCS** | 97.8% | ✅ Well covered |
| **MS-DRG** | 97.0% | ✅ Well covered |
| **APR-DRG** | 94.8% | ✅ Well covered |
| **APC** | 98.4% | ✅ Well covered |
| **RC** | 86.4% | ✅ Well covered |
| **NDC** | 40.5% | ⚠️ Partial (drugs) |
| **CDM** | 5.0% | 🔴 Hospital-specific codes |
| **ICD/Local** | 100% | ✅ Complete |

### CDM Code Challenge
- CDM codes are hospital-specific (no standard definitions)
- Priority: Low - less useful for price comparison
- Future: Generate from hospital descriptions in raw files

### SNOMED Descriptions
- User has access to SNOMED description sets
- Future: Create ingestion script for SNOMED data

---

## Files & Directories

| Path | Description |
|------|-------------|
| `hospital.db` | SQLite database (main data) |
| `data/downloads/` | Downloaded hospital files |
| `data/downloads/download_manifest.json` | Download status tracking |
| `data/michigan_hospitals_raw.json` | Source hospital list |
| `data/configs/` | AI-generated extraction configs |
| `data/config_manifest.json` | Validation status per hospital |
| `scripts/surveyor/` | Pipeline scripts |
| `scripts/create_fts_index.py` | FTS5 index builder |
| `src/main.py` | FastAPI backend |
| `src/static/index.html` | Frontend |

---

## Quick Commands

```bash
# Start the server
python3 run.py

# Rebuild FTS index (after reingestion)
python3 scripts/create_fts_index.py

# Run bulk ingestion
python3 scripts/surveyor/bulk_ingest.py

# Generate preview cards
python3 scripts/surveyor/preview_cards.py
```

---

## Notes
- Database deleted and reingesting for clean slate (Jan 2026)
- Beaumont hospitals are now "COREWELL HEALTH BEAUMONT" (2022 merger)
- U of M main hospital needs manual download (403 blocked)
- FTS5 index must be rebuilt after any reingestion
