# AI Data Cleaner & Normalizer

AI-assisted data cleaning pipelines for healthcare provider datasets. Uses NPI Registry lookups + AI resolvers to auto-discover missing credentials, normalize organization names, and align messy Excel columns — with confidence scoring on every record.

## Projects Inside

| Folder | Description |
|--------|-------------|
| `cred_org_fix/` | CRED_ORG_FIX — AI-powered credential discovery and org name normalization |
| `column_aligner/` | Medical data column alignment and standardization for large Excel files |

## CRED_ORG_FIX Pipeline

Takes a spreadsheet of healthcare providers and:
1. Looks up each provider in the **NPI Registry**
2. Uses an **AI resolver** to fill in missing credentials (MD, DO, RN, etc.)
3. Normalizes organization names to a standard format
4. Appends 3 new columns: `Discovered Credential`, `Discovered Organization`, `Confidence Score`
5. Saves progress after every row — safe to stop and restart

### Usage

```bash
pip install -r cred_org_fix/requirements.txt
# Place input file at cred_org_fix/input/providers_input.xlsx
python cred_org_fix/main_cleaning_pipeline.py
```

## Column Aligner

Processes large raw medical data Excel exports and aligns columns to a standard schema:
- Handles misaligned headers from different export formats
- Standardizes field names and data types
- Logs every transformation with before/after values

### Usage

```bash
pip install -r column_aligner/requirements.txt
python column_aligner/medical_aligner.py
```

## Key Features

- **Progress checkpointing** — resumes from last completed row on restart
- **Confidence scoring** — every AI-resolved record gets a confidence score (0–1)
- **NPI Registry integration** — authoritative source for healthcare provider data
- **Handles thousands of rows** — designed for large-scale batch processing

## Tech Stack

- Python, Pandas, openpyxl
- NPI Registry API
- Google Generative AI
- Colorama (progress display)

## Environment Variables

```
GOOGLE_API_KEY=your_gemini_key
NPI_API_BASE=https://npiregistry.cms.hhs.gov/api/
```
