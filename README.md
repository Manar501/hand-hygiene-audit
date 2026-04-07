# Hand Hygiene Audit Project

## Overview
This project automates the Hand Hygiene compliance audit workflow — from data collection to dashboard reporting. It replaces manual data entry and cleaning with a streamlined pipeline that saves time and reduces errors.

## How It Works

### 1. Data Collection (SmartSheet)
- A SmartSheet form link is shared with medical staff (Nurses, Physicians, Housekeeping, etc.) across all hospital departments (ICU, CCU, ER, etc.)
- Staff fill out the Hand Hygiene audit form directly from their phones or computers
- All responses are automatically collected in one SmartSheet

### 2. Data Cleaning (Python - Google Colab)
- The raw data is exported from SmartSheet as an Excel file
- A Python script in Google Colab automatically:
  - Combines repeated columns (Healthcare Worker Type 1-5, Opportunity 1-5, HH Action 1-5) into single columns
  - Fills empty cells using forward fill
  - Fixes date formats
  - Converts abbreviations to full text (e.g., "bef-pat" → "Before contact with the patient")
  - Removes unnecessary columns
  - Appends the new data to the existing master file
- The entire cleaning process takes **seconds** instead of hours of manual work

### 3. Dashboard Update (Power BI)
- The cleaned Excel file is connected to a Power BI dashboard
- Simply replace the old file with the updated one and click **Refresh**
- The dashboard updates automatically — no need to rebuild anything

## Project Files
| File | Description |
|------|-------------|
| `hand_hygiene_cleaning.ipynb` | Google Colab notebook — the Python cleaning script |
| `Hand Hygiene Dashboard.pbix` | Power BI Dashboard file |

## Tools Used
- **SmartSheet** — Data collection from staff
- **Python (pandas)** — Data cleaning and transformation
- **Google Colab** — Free cloud environment to run the Python script
- **Power BI** — Interactive dashboard for compliance reporting

## SmartSheet Link
[Click here to access the SmartSheet](https://app.smartsheet.com/b/form/019c89d7741278a4be147bf18af4e68b)

## Power BI Dashboard
[Click here to view the Dashboard](PASTE YOUR POWER BI LINK HERE)

## How to Update Data (Step by Step)
1. Export the new audit data from SmartSheet as `.xlsx`
2. Open the [Google Colab notebook](https://colab.research.google.com/drive/1KwjWXoDD3IxWFiOEMztojYuHg_UHeFII#scrollTo=_wnBW8R5VI-Y)
3. Run **Cell 1** — upload the new data file + the existing `jan.xlsx`
4. Run **Cell 2, 3, 4** — the script cleans the data and downloads the updated file
5. Replace the old `jan.xlsx` with the new one (same folder, same name)
6. Open Power BI and click **Refresh** — done!

## The Workflow
```
SmartSheet Form (Staff fills out)
        ↓
Export as Excel (.xlsx)
        ↓
Google Colab (Python cleans & combines)
        ↓
Updated jan.xlsx
        ↓
Power BI Dashboard (Refresh → Updated automatically)
```

## Key Benefits
- **No manual data entry** — staff submit directly through SmartSheet
- **Automatic cleaning** — Python handles all formatting, abbreviations, and errors
- **Fast updates** — entire process takes minutes instead of hours
- **Consistent data** — no human errors in data transformation
- **Real-time reporting** — Power BI dashboard always shows the latest compliance data
