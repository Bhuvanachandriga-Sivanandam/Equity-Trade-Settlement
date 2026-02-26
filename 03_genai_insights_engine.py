"""
Equity Trade Settlement - GenAI-Powered Insights Engine
Generates plain-English settlement summaries, root cause analysis,
and anomaly detection alerts from reconciliation data.

This module demonstrates GenAI integration for operations reporting.
For production: connect to Azure OpenAI or Anthropic API.
This version uses rule-based NLG (Natural Language Generation) to 
simulate GenAI output — can be swapped with LLM API calls.

Author: Bhuvanachandriga Sivanandam
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os

OUTPUT_DIR = '/home/claude/equity-trade-settlement/output'
INSIGHTS_DIR = '/home/claude/equity-trade-settlement/insights'
os.makedirs(INSIGHTS_DIR, exist_ok=True)

print("=" * 60)
print("GENAI-POWERED SETTLEMENT INSIGHTS ENGINE")
print("=" * 60)

# ============================================================
# LOAD RECONCILIATION DATA
# ============================================================
print("\n[1/5] Loading reconciliation data...")

df_recon = pd.read_csv(f'{OUTPUT_DIR}/reconciliation_report.csv')
df_breaks = pd.read_csv(f'{OUTPUT_DIR}/settlement_breaks.csv')
df_daily = pd.read_csv(f'{OUTPUT_DIR}/daily_break_trend.csv')
df_cp = pd.read_csv(f'{OUTPUT_DIR}/counterparty_summary.csv')
df_aging = pd.read_csv(f'{OUTPUT_DIR}/aging_summary.csv')

total_trades = len(df_recon)
total_breaks = len(df_breaks)
match_rate = (total_trades - total_breaks) / total_trades * 100
total_break_value = df_breaks['break_value'].sum()

# ============================================================
# INSIGHT 1: DAILY SETTLEMENT SUMMARY (GenAI-style)
# ============================================================
print("[2/5] Generating daily settlement summary...")

def generate_daily_summary():
    """Generate a plain-English daily settlement summary."""
    
    # Get most recent day's data
    df_daily_sorted = df_daily.sort_values('trade_date_str', ascending=False)
    latest = df_daily_sorted.iloc[0]
    prev = df_daily_sorted.iloc[1] if len(df_daily_sorted) > 1 else latest
    
    # Trend direction
    rate_change = latest['break_rate'] - prev['break_rate']
    trend = "increased" if rate_change > 0 else "decreased" if rate_change < 0 else "remained stable"
    
    # Top break counterparty
    top_cp = df_cp.sort_values('breaks', ascending=False).iloc[0]
    
    # Break type analysis
    break_types = df_breaks['break_type'].value_counts()
    top_break = break_types.index[0].split(' | ')[0] if len(break_types) > 0 else "N/A"
    
    summary = f"""
================================================================================
DAILY SETTLEMENT SUMMARY — {datetime.now().strftime('%B %d, %Y')}
================================================================================

EXECUTIVE OVERVIEW
------------------
Today's reconciliation processed {total_trades:,} equity trades across Internal, 
Custodian, and Exchange datasets. The overall settlement match rate stands at 
{match_rate:.1f}%, with {total_breaks:,} breaks identified totaling 
${total_break_value:,.0f} in notional exposure.

The break rate has {trend} compared to the previous trading day 
({latest['break_rate']:.1f}% vs {prev['break_rate']:.1f}%), representing a 
{abs(rate_change):.2f} percentage point {'increase' if rate_change > 0 else 'decrease'}.

KEY FINDINGS
------------
1. BREAK CONCENTRATION: The most common break type is {top_break.replace('_', ' ').title()}, 
   accounting for the largest share of exceptions. Operations teams should prioritize 
   investigation of these breaks first.

2. COUNTERPARTY RISK: {top_cp['counterparty_int']} has the highest break count 
   with {int(top_cp['breaks']):,} unresolved breaks valued at ${top_cp['total_break_value']:,.0f}. 
   Recommend escalating to the counterparty relationship manager for review.

3. AGING CONCERN: Breaks aging beyond 30 days require immediate attention as they 
   approach regulatory reporting thresholds and increase settlement risk exposure.

RECOMMENDED ACTIONS
-------------------
• Investigate all QUANTITY_MISMATCH breaks as priority — these represent the 
  highest value discrepancies and may indicate booking errors.
• Schedule counterparty reconciliation call with {top_cp['counterparty_int']} 
  to resolve outstanding breaks.
• Review all Failed settlements for potential systemic issues in the 
  custodian feed.
• Escalate breaks aging 90+ days to senior management for resolution 
  planning.
"""
    return summary

daily_summary = generate_daily_summary()
print(daily_summary)

# ============================================================
# INSIGHT 2: ROOT CAUSE ANALYSIS
# ============================================================
print("[3/5] Performing root cause analysis...")

def generate_root_cause_analysis():
    """Analyze break patterns and generate root cause narratives."""
    
    # Break type analysis
    all_break_types = []
    for bt in df_breaks['break_type']:
        all_break_types.extend(bt.split(' | '))
    
    from collections import Counter
    break_counts = Counter(all_break_types)
    total_break_instances = sum(break_counts.values())
    
    # Counterparty concentration
    cp_break_counts = df_breaks['counterparty_int'].value_counts()
    top_3_cp = cp_break_counts.head(3)
    top_3_pct = top_3_cp.sum() / total_breaks * 100
    
    # Trade type analysis
    tt_breaks = df_breaks['trade_type'].value_counts()
    highest_break_tt = tt_breaks.index[0]
    
    # Time pattern analysis
    df_breaks_copy = df_breaks.copy()
    df_breaks_copy['trade_date_dt'] = pd.to_datetime(df_breaks_copy['trade_date'], errors='coerce')
    df_breaks_copy['day_of_week'] = df_breaks_copy['trade_date_dt'].dt.day_name()
    dow_breaks = df_breaks_copy['day_of_week'].value_counts()
    peak_day = dow_breaks.index[0]
    
    analysis = f"""
================================================================================
ROOT CAUSE ANALYSIS — SETTLEMENT BREAKS
================================================================================

BREAK TYPE ANALYSIS
-------------------"""
    
    for bt, count in break_counts.most_common():
        pct = count / total_break_instances * 100
        
        if 'QUANTITY' in bt:
            cause = "Likely caused by partial fills, booking errors, or lot size discrepancies between internal and custodian systems."
            action = "Cross-reference with execution management system (EMS) fill reports."
        elif 'PRICE' in bt:
            cause = "May result from delayed price feeds, rounding differences, or currency conversion timing gaps."
            action = "Verify price source timestamps and FX rate application logic."
        elif 'COUNTERPARTY' in bt:
            cause = "Indicates counterparty name mapping inconsistencies between internal master data and custodian records."
            action = "Update counterparty reference data mapping table."
        elif 'DATE' in bt:
            cause = "Settlement date mismatches often arise from holiday calendar differences or T+2 calculation errors across jurisdictions."
            action = "Validate settlement calendar configuration across all systems."
        elif 'FAILED' in bt:
            cause = "Failed settlements typically indicate insufficient securities/cash, custodian processing errors, or rejected instructions."
            action = "Confirm custodian has received valid settlement instructions."
        elif 'EXCHANGE' in bt:
            cause = "Exchange quantity discrepancies suggest partial executions not properly aggregated in the internal blotter."
            action = "Reconcile individual fills from exchange feed against aggregated blotter entries."
        else:
            cause = "Requires manual investigation."
            action = "Assign to operations analyst for review."
        
        analysis += f"""
• {bt.replace('_', ' ').title()} — {count:,} instances ({pct:.1f}%)
  Root Cause: {cause}
  Action: {action}
"""
    
    analysis += f"""
PATTERN ANALYSIS
----------------
• Counterparty Concentration: Top 3 counterparties ({', '.join(top_3_cp.index[:3])}) 
  account for {top_3_pct:.0f}% of all breaks. This suggests systemic reconciliation 
  issues with specific counterparty feeds rather than random errors.

• Trade Type Pattern: '{highest_break_tt}' trades show the highest absolute break 
  count ({tt_breaks.iloc[0]:,}), which correlates with their higher trading volume.

• Temporal Pattern: Breaks peak on {peak_day}s ({dow_breaks.iloc[0]:,} breaks), 
  which may indicate end-of-week processing backlogs or batch job timing issues.

RISK ASSESSMENT
---------------
• Total notional value at risk from unresolved breaks: ${total_break_value:,.0f}
• Average break value: ${total_break_value/total_breaks:,.0f} per break
• Estimated resolution priority: HIGH for quantity and failed breaks, 
  MEDIUM for price and counterparty mismatches, LOW for date mismatches.
"""
    return analysis

rca = generate_root_cause_analysis()
print(rca)

# ============================================================
# INSIGHT 3: ANOMALY DETECTION ALERTS
# ============================================================
print("[4/5] Running anomaly detection...")

def generate_anomaly_alerts():
    """Detect anomalies in break patterns and generate alerts."""
    
    alerts = []
    
    # Alert 1: Break rate spike detection
    df_daily_sorted = df_daily.sort_values('trade_date_str')
    avg_break_rate = df_daily_sorted['break_rate'].mean()
    std_break_rate = df_daily_sorted['break_rate'].std()
    threshold = avg_break_rate + (2 * std_break_rate)
    
    spike_days = df_daily_sorted[df_daily_sorted['break_rate'] > threshold]
    if len(spike_days) > 0:
        alerts.append({
            'severity': 'HIGH',
            'type': 'BREAK_RATE_SPIKE',
            'message': f"{len(spike_days)} trading days exceeded the 2-sigma break rate threshold of {threshold:.1f}%. Average break rate is {avg_break_rate:.1f}%. Investigate batch processing or data feed issues on these dates.",
            'dates': spike_days['trade_date_str'].tolist()[:5]
        })
    
    # Alert 2: Counterparty concentration
    cp_breaks_pct = df_breaks['counterparty_int'].value_counts(normalize=True) * 100
    concentrated_cps = cp_breaks_pct[cp_breaks_pct > 10]
    if len(concentrated_cps) > 0:
        for cp, pct in concentrated_cps.items():
            alerts.append({
                'severity': 'MEDIUM',
                'type': 'COUNTERPARTY_CONCENTRATION',
                'message': f"{cp} accounts for {pct:.1f}% of all settlement breaks. This exceeds the 10% single-counterparty threshold. Recommend counterparty-level reconciliation review.",
                'counterparty': cp
            })
    
    # Alert 3: High-value breaks
    high_value_threshold = 1000000  # $1M
    high_value_breaks = df_breaks[df_breaks['break_value'] > high_value_threshold]
    if len(high_value_breaks) > 0:
        alerts.append({
            'severity': 'HIGH',
            'type': 'HIGH_VALUE_BREAKS',
            'message': f"{len(high_value_breaks):,} breaks exceed ${high_value_threshold/1e6:.0f}M in notional value, totaling ${high_value_breaks['break_value'].sum():,.0f}. These require immediate senior management attention.",
            'count': len(high_value_breaks)
        })
    
    # Alert 4: Aging breaks
    if 'aging_bucket' in df_breaks.columns:
        old_breaks = df_breaks[df_breaks['aging_bucket'].isin(['31-90 days', '90+ days'])]
        if len(old_breaks) > 0:
            alerts.append({
                'severity': 'HIGH',
                'type': 'AGING_BREAKS',
                'message': f"{len(old_breaks):,} breaks are aging beyond 30 days with ${old_breaks['break_value'].sum():,.0f} in notional exposure. These approach regulatory reporting thresholds and require escalation.",
                'count': len(old_breaks)
            })
    
    # Alert 5: Failed settlement trend
    failed = df_breaks[df_breaks['break_type'].str.contains('FAILED', na=False)]
    if len(failed) > 100:
        alerts.append({
            'severity': 'MEDIUM',
            'type': 'FAILED_SETTLEMENT_VOLUME',
            'message': f"{len(failed):,} failed settlements detected. Volume exceeds normal operational threshold. Check custodian connectivity and instruction delivery pipeline.",
            'count': len(failed)
        })
    
    return alerts

alerts = generate_anomaly_alerts()

alert_text = """
================================================================================
ANOMALY DETECTION ALERTS
================================================================================
"""

for i, alert in enumerate(alerts, 1):
    severity_icon = "🔴" if alert['severity'] == 'HIGH' else "🟡" if alert['severity'] == 'MEDIUM' else "🟢"
    alert_text += f"""
ALERT #{i} [{alert['severity']}] — {alert['type'].replace('_', ' ')}
{'-' * 60}
{alert['message']}
"""

print(alert_text)

# ============================================================
# INSIGHT 4: COPILOT STUDIO KNOWLEDGE BASE
# ============================================================
print("[5/5] Generating Copilot Studio knowledge base...")

def generate_copilot_knowledge_base():
    """Generate structured Q&A pairs for Copilot Studio agent."""
    
    # Top counterparties by breaks
    cp_top5 = df_cp.sort_values('breaks', ascending=False).head(5)
    
    # Break type stats
    all_break_types = []
    for bt in df_breaks['break_type']:
        all_break_types.extend(bt.split(' | '))
    from collections import Counter
    break_counts = Counter(all_break_types)
    
    knowledge_base = {
        "overview": {
            "total_trades": int(total_trades),
            "total_breaks": int(total_breaks),
            "match_rate": round(match_rate, 1),
            "total_break_value": round(float(total_break_value), 2),
            "report_date": datetime.now().strftime('%Y-%m-%d')
        },
        "break_types": {
            bt.lower(): {"count": count, "percentage": round(count/sum(break_counts.values())*100, 1)}
            for bt, count in break_counts.most_common()
        },
        "counterparty_breaks": {
            row['counterparty_int']: {
                "total_trades": int(row['total_trades']),
                "breaks": int(row['breaks']),
                "match_rate": round(row['match_rate'], 1),
                "break_value": round(row['total_break_value'], 0)
            }
            for _, row in cp_top5.iterrows()
        },
        "faq_pairs": [
            {
                "question": "What is today's settlement match rate?",
                "answer": f"The current settlement match rate is {match_rate:.1f}%. Out of {total_trades:,} trades processed, {total_breaks:,} breaks were identified."
            },
            {
                "question": "Which counterparty has the most settlement breaks?",
                "answer": f"{cp_top5.iloc[0]['counterparty_int']} has the highest break count with {int(cp_top5.iloc[0]['breaks']):,} breaks valued at ${cp_top5.iloc[0]['total_break_value']:,.0f}."
            },
            {
                "question": "What are the most common types of breaks?",
                "answer": f"The top break types are: " + ", ".join([f"{bt.replace('_', ' ').title()} ({count:,})" for bt, count in break_counts.most_common(3)])
            },
            {
                "question": "Show me all unmatched trades over $1M",
                "answer": f"There are {len(df_breaks[df_breaks['break_value'] > 1000000]):,} breaks exceeding $1M in notional value. These are flagged as HIGH priority for immediate investigation."
            },
            {
                "question": "What is the break aging distribution?",
                "answer": "Breaks are distributed across aging buckets. Breaks beyond 30 days require escalation to senior management per operational risk policy."
            },
            {
                "question": "Are there any anomalies today?",
                "answer": f"The anomaly detection engine has identified {len(alerts)} active alerts. " + " ".join([f"[{a['severity']}] {a['type'].replace('_', ' ')}" for a in alerts[:3]])
            }
        ]
    }
    
    return knowledge_base

kb = generate_copilot_knowledge_base()

# Save knowledge base for Copilot Studio
with open(f'{INSIGHTS_DIR}/copilot_knowledge_base.json', 'w') as f:
    json.dump(kb, f, indent=2)

# ============================================================
# SAVE ALL INSIGHTS
# ============================================================
with open(f'{INSIGHTS_DIR}/daily_settlement_summary.txt', 'w') as f:
    f.write(daily_summary)

with open(f'{INSIGHTS_DIR}/root_cause_analysis.txt', 'w') as f:
    f.write(rca)

with open(f'{INSIGHTS_DIR}/anomaly_alerts.json', 'w') as f:
    json.dump(alerts, f, indent=2, default=str)

with open(f'{INSIGHTS_DIR}/anomaly_alerts.txt', 'w') as f:
    f.write(alert_text)

print(f"\nAll insights saved to: {INSIGHTS_DIR}/")
print(f"   - daily_settlement_summary.txt")
print(f"   - root_cause_analysis.txt")
print(f"   - anomaly_alerts.json")
print(f"   - anomaly_alerts.txt")
print(f"   - copilot_knowledge_base.json (for Copilot Studio)")

print("\n" + "=" * 60)
print("GENAI INSIGHTS ENGINE COMPLETE")
print("=" * 60)
