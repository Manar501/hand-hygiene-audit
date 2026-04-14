"""
Hand Hygiene Audit — Automated ETL Pipeline
Extracts data from SmartSheet API, cleans it, and saves to Excel for Power BI.
Runs daily via Windows Task Scheduler at 7:00 AM.

Authors: Manar, Norah
"""

import smartsheet
import pandas as pd
import datetime
import os
from datetime import date

# ============================================
# CONFIGURATION — UPDATE THESE VALUES
# ============================================
# Get API Key: SmartSheet → Profile → Personal Settings → API Access → Generate Token
SMARTSHEET_API_KEY = 'YOUR_API_KEY_HERE'

# Get Sheet ID: SmartSheet → File → Properties → Sheet ID
SMARTSHEET_SHEET_ID = 'YOUR_SHEET_ID_HERE'

# Path where Power BI reads the file — update to match your setup
SAVE_PATH = r'C:\Users\YOUR_USERNAME\Desktop\YOUR_FOLDER\jan.xlsx'
BACKUP_FOLDER = r'C:\Users\YOUR_USERNAME\Desktop\YOUR_FOLDER\backups'

os.makedirs(BACKUP_FOLDER, exist_ok=True)

# ============================================
# STEP 1: READ EXISTING DATA FIRST
# ============================================
if os.path.exists(SAVE_PATH):
    print('Reading existing jan.xlsx...')
    existing_df = pd.read_excel(SAVE_PATH)
    existing_df['AUDIT DATE'] = pd.to_datetime(existing_df['AUDIT DATE'], errors='coerce').dt.strftime('%m/%d/%Y')
    print(f'Existing rows: {len(existing_df)}')
else:
    print('No existing file found')
    existing_df = pd.DataFrame()

# ============================================
# STEP 2: BACKUP EXISTING FILE
# ============================================
if os.path.exists(SAVE_PATH):
    backup_name = os.path.join(BACKUP_FOLDER, f'jan_backup_{date.today().strftime("%Y%m%d")}.xlsx')
    existing_df.to_excel(backup_name, index=False, sheet_name='JAN 26')
    print(f'Backup saved: {backup_name}')

# ============================================
# STEP 3: EXTRACT FROM SMARTSHEET
# ============================================
print('Connecting to SmartSheet...')
client = smartsheet.Smartsheet(SMARTSHEET_API_KEY)
client.errors_as_exceptions(True)
sheet = client.Sheets.get_sheet(int(SMARTSHEET_SHEET_ID))
print(f'Sheet: {sheet.name} — {len(sheet.rows)} rows')

columns = {col.id: col.title for col in sheet.columns}
data = []
for row in sheet.rows:
    row_data = {}
    for cell in row.cells:
        row_data[columns.get(cell.column_id, 'Unknown')] = cell.value
    data.append(row_data)
raw_df = pd.DataFrame(data)

# ============================================
# STEP 4: CLEAN NEW DATA
# ============================================
print('Cleaning new data...')

# Handle both formats: repeated columns (1-5) or already combined
if 'Healthcare Worker Type (1)' in raw_df.columns:
    base = ['DEPARTMENT', 'AUDIT DATE', 'ADUIT TIME']
    frames = []
    for i in range(1, 6):
        hw = f'Healthcare Worker Type ({i})'
        opp = f'Opp. Indication ({i})'
        hh = f'HH Action ({i})'
        if hw in raw_df.columns:
            temp = raw_df[base + [hw, opp, hh]].copy()
            temp.columns = base + ['Healthcare Worker Type', 'Opportunity', 'Hand Hygiene Action']
            frames.append(temp)
    new_df = pd.concat(frames, ignore_index=True)
else:
    new_df = raw_df[['DEPARTMENT', 'AUDIT DATE', 'ADUIT TIME', 'Healthcare Worker Type', 'Opportunity', 'Hand Hygiene Action']].copy()

# Convert dates — DELETE rows with no date (no forward fill on dates)
new_df['AUDIT DATE'] = pd.to_datetime(new_df['AUDIT DATE'], errors='coerce')
new_df = new_df.dropna(subset=['AUDIT DATE'])

# Drop rows where all 3 main columns are empty
new_df.dropna(subset=['Healthcare Worker Type', 'Opportunity', 'Hand Hygiene Action'], how='all', inplace=True)

# Clean whitespace
for col in ['Opportunity', 'Hand Hygiene Action', 'Healthcare Worker Type', 'DEPARTMENT', 'ADUIT TIME']:
    new_df[col] = new_df[col].astype(str).str.strip()
    new_df[col] = new_df[col].replace({'nan': pd.NA, 'None': pd.NA, '': pd.NA})

# Drop empty rows again after cleaning
new_df.dropna(subset=['Healthcare Worker Type', 'Opportunity', 'Hand Hygiene Action'], how='all', inplace=True)

# Convert WHO 5 Moments abbreviations to full text
opp_map = {
    'bef-pat': 'Before contact with the patient',
    'aft-pat': 'After contact with the patient',
    'aft.p.surr': "After contact with the patient's surroundings",
    'bef-asept': 'Before aseptic/clean procedure',
    'aft-b.f.': 'After blood/body fluids exposure risk'
}
new_df['Opportunity'] = new_df['Opportunity'].map(opp_map).fillna(new_df['Opportunity'])

# Format dates
new_df['AUDIT DATE'] = new_df['AUDIT DATE'].dt.strftime('%m/%d/%Y')
new_df.reset_index(drop=True, inplace=True)

print(f'New clean rows: {len(new_df)}')

# ============================================
# STEP 5: MERGE — KEEP ALL OLD ROWS + ADD ONLY NEW DATES
# ============================================
print('Merging old data with new data...')

if len(existing_df) > 0:
    old_dates = set(existing_df['AUDIT DATE'].dropna().unique())
    new_rows = new_df[~new_df['AUDIT DATE'].isin(old_dates)]
    combined = pd.concat([existing_df, new_rows], ignore_index=True)
    print(f'Old rows kept: {len(existing_df)}')
    print(f'New rows added: {len(new_rows)}')
else:
    combined = new_df.copy()

print(f'Total rows: {len(combined)}')

# ============================================
# STEP 6: SAVE
# ============================================
combined.to_excel(SAVE_PATH, index=False, sheet_name='JAN 26')
print(f'Done! {len(combined)} rows saved to {SAVE_PATH}')
