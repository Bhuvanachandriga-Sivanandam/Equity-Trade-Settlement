"""
Equity Trade Settlement - Reconciliation Engine
Performs 3-way reconciliation across Internal, Custodian, and Exchange datasets.
Detects and categorizes settlement breaks.

Designed for Azure Databricks (PySpark + Delta Lake SQL).
This script uses Pandas for local execution — see PySpark equivalents in comments.

Author: Bhuvanachandriga Sivanandam
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import warnings
warnings.filterwarnings('ignore')

# ============================================================
# CONFIGURATION
# ============================================================
DATA_DIR = '/home/claude/equity-trade-settlement/data'
OUTPUT_DIR = '/home/claude/equity-trade-settlement/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

TOLERANCE_PRICE = 0.01      # $0.01 tolerance for price matching
TOLERANCE_QUANTITY = 0       # Exact match required for quantity

print("=" * 60)
print("EQUITY TRADE SETTLEMENT - RECONCILIATION ENGINE")
print("=" * 60)

# ============================================================
# STEP 1: LOAD DATA
# ============================================================
# PySpark equivalent:
# df_internal = spark.read.format("delta").load("/mnt/delta/internal_trades")
# df_custodian = spark.read.format("delta").load("/mnt/delta/custodian_records")
# df_exchange = spark.read.format("delta").load("/mnt/delta/exchange_confirms")

print("\n[STEP 1] Loading datasets...")

df_internal = pd.read_csv(f'{DATA_DIR}/internal_trade_blotter.csv')
df_custodian = pd.read_csv(f'{DATA_DIR}/custodian_settlement_records.csv')
df_exchange = pd.read_csv(f'{DATA_DIR}/exchange_confirmation_feed.csv')

print(f"   Internal Blotter:    {len(df_internal):,} records")
print(f"   Custodian Records:   {len(df_custodian):,} records")
print(f"   Exchange Confirms:   {len(df_exchange):,} records")

# ============================================================
# STEP 2: THREE-WAY RECONCILIATION
# ============================================================
# PySpark equivalent:
# df_recon = df_internal.alias("i") \
#     .join(df_custodian.alias("c"), on="trade_id", how="outer") \
#     .join(df_exchange.alias("e"), on="trade_id", how="outer")

print("\n[STEP 2] Performing 3-way reconciliation...")

# Merge Internal ↔ Custodian
df_recon = df_internal.merge(
    df_custodian[['trade_id', 'quantity', 'price', 'counterparty', 
                   'settlement_date', 'settlement_status', 'custodian_ref', 'notional_value']],
    on='trade_id', how='outer', suffixes=('_int', '_cust')
)

# Merge with Exchange
df_recon = df_recon.merge(
    df_exchange[['trade_id', 'quantity', 'price', 'exchange_ref', 'exchange', 'execution_venue']],
    on='trade_id', how='outer', suffixes=('', '_exch')
)

# Rename exchange columns clearly
df_recon.rename(columns={
    'quantity': 'quantity_exch', 
    'price': 'price_exch'
}, inplace=True)

print(f"   Reconciled records:  {len(df_recon):,}")

# ============================================================
# STEP 3: BREAK DETECTION
# ============================================================
print("\n[STEP 3] Detecting settlement breaks...")

def detect_break(row):
    """Categorize each trade's reconciliation status."""
    breaks = []
    
    # Check quantity mismatch (Internal vs Custodian)
    if pd.notna(row.get('quantity_int')) and pd.notna(row.get('quantity_cust')):
        if abs(row['quantity_int'] - row['quantity_cust']) > TOLERANCE_QUANTITY:
            breaks.append('QUANTITY_MISMATCH')
    
    # Check price mismatch (Internal vs Custodian)
    if pd.notna(row.get('price_int')) and pd.notna(row.get('price_cust')):
        if abs(row['price_int'] - row['price_cust']) > TOLERANCE_PRICE:
            breaks.append('PRICE_MISMATCH')
    
    # Check counterparty mismatch
    if pd.notna(row.get('counterparty_int')) and pd.notna(row.get('counterparty_cust')):
        if row['counterparty_int'] != row['counterparty_cust']:
            breaks.append('COUNTERPARTY_MISMATCH')
    
    # Check settlement date mismatch
    if pd.notna(row.get('settlement_date_int')) and pd.notna(row.get('settlement_date_cust')):
        if row['settlement_date_int'] != row['settlement_date_cust']:
            breaks.append('DATE_MISMATCH')
    
    # Check for missing/failed records
    if row.get('settlement_status') == 'Failed':
        breaks.append('SETTLEMENT_FAILED')
    
    # Check quantity vs exchange
    if pd.notna(row.get('quantity_int')) and pd.notna(row.get('quantity_exch')):
        if abs(row['quantity_int'] - row['quantity_exch']) > TOLERANCE_QUANTITY:
            breaks.append('EXCHANGE_QTY_MISMATCH')
    
    if len(breaks) == 0:
        return 'MATCHED'
    else:
        return ' | '.join(breaks)

df_recon['break_type'] = df_recon.apply(detect_break, axis=1)
df_recon['is_break'] = df_recon['break_type'] != 'MATCHED'

# Calculate value at risk for breaks
df_recon['break_value'] = np.where(
    df_recon['is_break'],
    df_recon['notional_value_int'].fillna(0),
    0
)

# ============================================================
# STEP 4: BREAK AGING
# ============================================================
print("[STEP 4] Calculating break aging...")

today = pd.to_datetime('2025-12-31')
df_recon['trade_date_dt'] = pd.to_datetime(df_recon['trade_date'], errors='coerce')
df_recon['days_outstanding'] = (today - df_recon['trade_date_dt']).dt.days

def aging_bucket(days):
    if pd.isna(days): return 'Unknown'
    if days <= 1: return 'T+0 to T+1'
    if days <= 3: return 'T+2 to T+3'
    if days <= 7: return '4-7 days'
    if days <= 30: return '8-30 days'
    if days <= 90: return '31-90 days'
    return '90+ days'

df_recon['aging_bucket'] = df_recon['days_outstanding'].apply(aging_bucket)

# ============================================================
# STEP 5: SUMMARY ANALYTICS
# ============================================================
print("[STEP 5] Generating summary analytics...\n")

total_trades = len(df_recon)
total_breaks = df_recon['is_break'].sum()
match_rate = (total_trades - total_breaks) / total_trades * 100
total_break_value = df_recon['break_value'].sum()

print("=" * 60)
print("RECONCILIATION RESULTS")
print("=" * 60)
print(f"\nTotal Trades Processed:    {total_trades:,}")
print(f"Matched Trades:            {total_trades - total_breaks:,}")
print(f"Settlement Breaks:         {total_breaks:,}")
print(f"Match Rate:                {match_rate:.1f}%")
print(f"Total Break Value (USD):   ${total_break_value:,.2f}")

# Break type distribution
print(f"\n--- Break Type Distribution ---")
break_records = df_recon[df_recon['is_break']]
all_break_types = []
for bt in break_records['break_type']:
    all_break_types.extend(bt.split(' | '))

from collections import Counter
break_counts = Counter(all_break_types)
for bt, count in break_counts.most_common():
    print(f"   {bt:30s}: {count:,}")

# Break by counterparty
print(f"\n--- Top 10 Counterparties by Break Count ---")
cp_breaks = break_records.groupby('counterparty_int').agg(
    break_count=('is_break', 'sum'),
    break_value=('break_value', 'sum')
).sort_values('break_count', ascending=False).head(10)

for cp, row in cp_breaks.iterrows():
    print(f"   {cp:25s}: {int(row['break_count']):,} breaks  |  ${row['break_value']:,.0f}")

# Break by aging
print(f"\n--- Break Aging Distribution ---")
aging = break_records['aging_bucket'].value_counts()
for bucket in ['T+0 to T+1', 'T+2 to T+3', '4-7 days', '8-30 days', '31-90 days', '90+ days']:
    if bucket in aging.index:
        print(f"   {bucket:15s}: {aging[bucket]:,}")

# Settlement status
print(f"\n--- Settlement Status Distribution ---")
for status, count in df_recon['settlement_status'].value_counts().items():
    print(f"   {status:15s}: {count:,} ({count/total_trades*100:.1f}%)")

# ============================================================
# STEP 6: SAVE OUTPUTS
# ============================================================
print(f"\n[STEP 6] Saving outputs...")

# Full reconciliation report
df_recon.to_csv(f'{OUTPUT_DIR}/reconciliation_report.csv', index=False)

# Breaks only
break_records.to_csv(f'{OUTPUT_DIR}/settlement_breaks.csv', index=False)

# Summary by counterparty
cp_summary = df_recon.groupby('counterparty_int').agg(
    total_trades=('trade_id', 'count'),
    breaks=('is_break', 'sum'),
    match_rate=('is_break', lambda x: (1 - x.mean()) * 100),
    total_break_value=('break_value', 'sum')
).round(1).sort_values('breaks', ascending=False)
cp_summary.to_csv(f'{OUTPUT_DIR}/counterparty_summary.csv')

# Summary by trade type
tt_summary = df_recon.groupby('trade_type').agg(
    total_trades=('trade_id', 'count'),
    breaks=('is_break', 'sum'),
    match_rate=('is_break', lambda x: (1 - x.mean()) * 100),
    total_break_value=('break_value', 'sum')
).round(1)
tt_summary.to_csv(f'{OUTPUT_DIR}/trade_type_summary.csv')

# Daily break trend
df_recon['trade_date_str'] = df_recon['trade_date']
daily_trend = df_recon.groupby('trade_date_str').agg(
    total_trades=('trade_id', 'count'),
    breaks=('is_break', 'sum'),
    break_value=('break_value', 'sum')
).reset_index()
daily_trend['break_rate'] = (daily_trend['breaks'] / daily_trend['total_trades'] * 100).round(2)
daily_trend.to_csv(f'{OUTPUT_DIR}/daily_break_trend.csv', index=False)

# Aging summary
aging_summary = break_records.groupby('aging_bucket').agg(
    count=('trade_id', 'count'),
    total_value=('break_value', 'sum')
).round(0)
aging_summary.to_csv(f'{OUTPUT_DIR}/aging_summary.csv')

print(f"\n   All outputs saved to: {OUTPUT_DIR}/")
print(f"   - reconciliation_report.csv")
print(f"   - settlement_breaks.csv")
print(f"   - counterparty_summary.csv")
print(f"   - trade_type_summary.csv")
print(f"   - daily_break_trend.csv")
print(f"   - aging_summary.csv")
print("\n" + "=" * 60)
print("RECONCILIATION COMPLETE")
print("=" * 60)
