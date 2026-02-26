"""
Equity Trade Settlement - Synthetic Data Generator
Generates 50,000+ equity trade records across 3 datasets:
  1. Internal Trade Blotter (front office)
  2. Custodian Settlement Records (back office)
  3. Exchange Confirmation Feed (external)

Author: Bhuvanachandriga Sivanandam
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random

np.random.seed(42)

# --- Configuration ---
NUM_TRADES = 50000
BREAK_RATE = 0.08  # 8% intentional breaks

TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 
            'BAC', 'GS', 'MS', 'C', 'WFC', 'BRK.B', 'V', 'MA', 'HD', 'DIS',
            'NFLX', 'CRM', 'ORCL', 'INTC', 'AMD', 'PYPL', 'SQ']

COUNTERPARTIES = ['Goldman Sachs', 'Morgan Stanley', 'JP Morgan', 'Citi', 
                  'Barclays', 'Deutsche Bank', 'UBS', 'Credit Suisse',
                  'BNP Paribas', 'HSBC', 'RBC Capital', 'TD Securities',
                  'BMO Capital', 'Scotia Capital', 'CIBC World Markets']

TRADE_TYPES = ['Buy', 'Sell', 'Short Sell', 'Buy to Cover']
SETTLEMENT_STATUSES = ['Matched', 'Unmatched', 'Partial Match', 'Failed', 'Pending']
CURRENCIES = ['USD', 'CAD', 'GBP', 'EUR']

# Date range: last 6 months of trading days
end_date = datetime(2025, 12, 31)
start_date = end_date - timedelta(days=180)
trading_days = pd.bdate_range(start_date, end_date)

print("=" * 60)
print("EQUITY TRADE SETTLEMENT - SYNTHETIC DATA GENERATOR")
print("=" * 60)

# --- Generate Base Trade Data ---
print("\n[1/4] Generating base trade records...")

trade_ids = [f"TRD-{str(i).zfill(7)}" for i in range(1, NUM_TRADES + 1)]
trade_dates = np.random.choice(trading_days, NUM_TRADES)
settlement_dates = trade_dates + pd.Timedelta(days=2)  # T+2 settlement

base_data = {
    'trade_id': trade_ids,
    'trade_date': trade_dates,
    'settlement_date': settlement_dates,
    'ticker': np.random.choice(TICKERS, NUM_TRADES),
    'trade_type': np.random.choice(TRADE_TYPES, NUM_TRADES, p=[0.40, 0.35, 0.15, 0.10]),
    'quantity': np.random.choice([100, 200, 500, 1000, 2000, 5000, 10000], NUM_TRADES),
    'price': np.round(np.random.uniform(10, 500, NUM_TRADES), 2),
    'counterparty': np.random.choice(COUNTERPARTIES, NUM_TRADES),
    'currency': np.random.choice(CURRENCIES, NUM_TRADES, p=[0.60, 0.20, 0.10, 0.10]),
}

df_base = pd.DataFrame(base_data)
df_base['notional_value'] = df_base['quantity'] * df_base['price']
df_base['trade_date'] = df_base['trade_date'].dt.strftime('%Y-%m-%d')
df_base['settlement_date'] = df_base['settlement_date'].dt.strftime('%Y-%m-%d')

print(f"   Generated {len(df_base):,} base trade records")

# --- Dataset 1: Internal Trade Blotter ---
print("[2/4] Creating Internal Trade Blotter...")

df_internal = df_base.copy()
df_internal['source'] = 'INTERNAL'
df_internal['trader_id'] = [f"TDR-{np.random.randint(100, 999)}" for _ in range(NUM_TRADES)]
df_internal['desk'] = np.random.choice(['Equity Flow', 'Equity Derivatives', 'Index Arb', 'Program Trading'], NUM_TRADES)
df_internal['booking_time'] = [f"{np.random.randint(9,16):02d}:{np.random.randint(0,59):02d}:{np.random.randint(0,59):02d}" for _ in range(NUM_TRADES)]

# --- Dataset 2: Custodian Settlement Records ---
print("[3/4] Creating Custodian Settlement Records...")

df_custodian = df_base[['trade_id', 'trade_date', 'settlement_date', 'ticker', 
                         'trade_type', 'quantity', 'price', 'counterparty', 
                         'currency', 'notional_value']].copy()
df_custodian['source'] = 'CUSTODIAN'
df_custodian['custodian_ref'] = [f"CUST-{np.random.randint(100000, 999999)}" for _ in range(NUM_TRADES)]
df_custodian['settlement_status'] = np.random.choice(
    SETTLEMENT_STATUSES, NUM_TRADES, p=[0.72, 0.08, 0.05, 0.10, 0.05]
)

# Introduce intentional breaks in custodian data
num_breaks = int(NUM_TRADES * BREAK_RATE)
break_indices = np.random.choice(NUM_TRADES, num_breaks, replace=False)

break_types = np.random.choice(
    ['quantity_mismatch', 'price_mismatch', 'missing_record', 'counterparty_mismatch', 'date_mismatch'],
    num_breaks, p=[0.30, 0.25, 0.20, 0.15, 0.10]
)

for idx, break_type in zip(break_indices, break_types):
    if break_type == 'quantity_mismatch':
        df_custodian.loc[idx, 'quantity'] = df_custodian.loc[idx, 'quantity'] + np.random.choice([-100, 100, -200, 200])
        df_custodian.loc[idx, 'settlement_status'] = 'Unmatched'
    elif break_type == 'price_mismatch':
        df_custodian.loc[idx, 'price'] = round(df_custodian.loc[idx, 'price'] * np.random.uniform(0.98, 1.02), 2)
        df_custodian.loc[idx, 'settlement_status'] = 'Partial Match'
    elif break_type == 'missing_record':
        df_custodian.loc[idx, 'settlement_status'] = 'Failed'
        df_custodian.loc[idx, 'quantity'] = 0
    elif break_type == 'counterparty_mismatch':
        df_custodian.loc[idx, 'counterparty'] = np.random.choice(COUNTERPARTIES)
        df_custodian.loc[idx, 'settlement_status'] = 'Unmatched'
    elif break_type == 'date_mismatch':
        orig_date = pd.to_datetime(df_custodian.loc[idx, 'settlement_date'])
        df_custodian.loc[idx, 'settlement_date'] = (orig_date + timedelta(days=np.random.randint(1, 5))).strftime('%Y-%m-%d')
        df_custodian.loc[idx, 'settlement_status'] = 'Pending'

# Recalculate notional for mismatched records
df_custodian['notional_value'] = df_custodian['quantity'] * df_custodian['price']

# --- Dataset 3: Exchange Confirmation Feed ---
print("[4/4] Creating Exchange Confirmation Feed...")

df_exchange = df_base[['trade_id', 'trade_date', 'ticker', 'trade_type', 
                        'quantity', 'price', 'currency', 'notional_value']].copy()
df_exchange['source'] = 'EXCHANGE'
df_exchange['exchange_ref'] = [f"EXC-{np.random.randint(1000000, 9999999)}" for _ in range(NUM_TRADES)]
df_exchange['exchange'] = np.random.choice(['NYSE', 'NASDAQ', 'TSX', 'LSE'], NUM_TRADES, p=[0.40, 0.35, 0.15, 0.10])
df_exchange['execution_venue'] = np.random.choice(['Lit Pool', 'Dark Pool', 'Auction', 'OTC'], NUM_TRADES, p=[0.50, 0.25, 0.15, 0.10])

# --- Save to CSV ---
output_dir = '/home/claude/equity-trade-settlement/data'
os.makedirs(output_dir, exist_ok=True)

df_internal.to_csv(f'{output_dir}/internal_trade_blotter.csv', index=False)
df_custodian.to_csv(f'{output_dir}/custodian_settlement_records.csv', index=False)
df_exchange.to_csv(f'{output_dir}/exchange_confirmation_feed.csv', index=False)

# --- Summary Stats ---
print("\n" + "=" * 60)
print("DATA GENERATION COMPLETE")
print("=" * 60)
print(f"\nTotal Records Per Dataset: {NUM_TRADES:,}")
print(f"Intentional Breaks:        {num_breaks:,} ({BREAK_RATE*100:.0f}%)")
print(f"\nBreak Distribution:")
for bt in ['quantity_mismatch', 'price_mismatch', 'missing_record', 'counterparty_mismatch', 'date_mismatch']:
    count = sum(1 for b in break_types if b == bt)
    print(f"   {bt:25s}: {count:,}")

print(f"\nSettlement Status Distribution (Custodian):")
for status, count in df_custodian['settlement_status'].value_counts().items():
    print(f"   {status:15s}: {count:,} ({count/NUM_TRADES*100:.1f}%)")

print(f"\nFiles saved to: {output_dir}/")
print(f"   - internal_trade_blotter.csv      ({df_internal.shape[0]:,} rows, {df_internal.shape[1]} cols)")
print(f"   - custodian_settlement_records.csv ({df_custodian.shape[0]:,} rows, {df_custodian.shape[1]} cols)")
print(f"   - exchange_confirmation_feed.csv   ({df_exchange.shape[0]:,} rows, {df_exchange.shape[1]} cols)")
