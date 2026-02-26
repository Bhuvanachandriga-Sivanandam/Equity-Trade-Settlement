# Equity Trade Settlement — Reconciliation & AI Insights Tool

**Automated 3-way reconciliation engine with GenAI-powered insights and Microsoft Copilot Studio integration for equity trade settlement operations.**

## Overview

This tool processes 50,000+ synthetic equity trade records across three data sources (Internal Trade Blotter, Custodian Settlement Records, and Exchange Confirmation Feed) to detect settlement breaks, perform root cause analysis, and generate plain-English operational insights.

Built for operations managers and settlement teams who need real-time visibility into trade matching, exception management, and counterparty risk.

## Architecture

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  Internal Trade   │    │   Custodian       │    │   Exchange       │
│  Blotter          │    │   Settlement      │    │   Confirmation   │
│  (Front Office)   │    │   Records         │    │   Feed           │
└────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘
         │                       │                       │
         └───────────┬───────────┴───────────┬───────────┘
                     │                       │
              ┌──────▼───────────────────────▼──────┐
              │    3-Way Reconciliation Engine       │
              │    (Python / Pandas)                 │
              └──────────────┬──────────────────────┘
                             │
              ┌──────────────▼──────────────────────┐
              │    Break Detection & Categorization  │
              │    • Quantity Mismatch               │
              │    • Price Mismatch                  │
              │    • Counterparty Mismatch           │
              │    • Settlement Date Mismatch        │
              │    • Failed Settlements              │
              └──────────────┬──────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
   ┌─────▼─────┐     ┌──────▼──────┐     ┌──────▼──────┐
   │  GenAI     │     │  Power BI   │     │  Copilot    │
   │  Insights  │     │  Dashboard  │     │  Studio     │
   │  Engine    │     │             │     │  Agent      │
   └───────────┘     └─────────────┘     └─────────────┘
```

## Tech Stack

| Component | Technology | Status |
|-----------|-----------|--------|
| Data Generation | Python, NumPy, Pandas | ✅ Implemented |
| Reconciliation Engine | Python, Pandas | ✅ Implemented |
| GenAI Insights | Python (Rule-based NLG, Anomaly Detection) | ✅ Implemented |
| Copilot Agent | Microsoft Copilot Studio | ✅ Knowledge base generated, agent setup in progress |
| Visualization | Power BI (DAX, Data Modeling) | 🔄 Data ready for import |
| Cloud Deployment | Azure Databricks, PySpark, Delta Lake | 📋 Designed — PySpark migration path documented in code |
| Version Control | Git, GitHub | ✅ Implemented |

> **Note:** The reconciliation engine is built in Python/Pandas for local execution. PySpark equivalents are included as comments throughout the code for Azure Databricks deployment. The architecture is designed to be production-deployable on Databricks with Delta Lake as the storage layer.

## Components

### 1. Synthetic Data Generator (`01_generate_synthetic_data.py`)
- Generates 50,000+ realistic equity trade records using Python
- Creates 3 separate datasets simulating Internal, Custodian, and Exchange feeds
- Business logic includes T+2 settlement rules, realistic counterparty names, and real equity tickers
- Introduces controlled settlement breaks (~8%) across 5 break types to simulate real-world reconciliation scenarios
- Break patterns designed based on common settlement failure modes in banking operations

### 2. Reconciliation Engine (`02_reconciliation_engine.py`)
- Performs 3-way reconciliation across all data sources
- Detects and categorizes 6 types of settlement breaks
- Calculates break aging buckets and notional value at risk
- Generates counterparty-level, trade-type, and daily trend analytics
- PySpark-equivalent code included in comments for Azure Databricks migration

### 3. GenAI Insights Engine (`03_genai_insights_engine.py`)
- **Daily Settlement Summary**: Plain-English executive overview with key findings and recommended actions
- **Root Cause Analysis**: Pattern-based analysis of break types with actionable remediation steps
- **Anomaly Detection**: Automated alerts for break rate spikes, counterparty concentration, high-value breaks, and aging thresholds
- **Copilot Studio Knowledge Base**: Structured JSON output for natural language querying
- Built using rule-based Natural Language Generation; designed to integrate with Azure OpenAI or Anthropic API for production use

### 4. Microsoft Copilot Studio Agent (Setup Guide: `COPILOT_STUDIO_SETUP.md`)
- Knowledge base JSON generated from reconciliation output
- Natural language interface for operations managers
- Pre-configured topics for common queries:
  - "What is today's settlement match rate?"
  - "Which counterparty has the most fails?"
  - "Show me breaks over $1M"
  - "What are the main causes of breaks?"
  - "Are there any anomalies?"

## Quick Start

```bash
# Step 1: Generate synthetic data
python 01_generate_synthetic_data.py

# Step 2: Run reconciliation
python 02_reconciliation_engine.py

# Step 3: Generate GenAI insights
python 03_genai_insights_engine.py

# Step 4: Set up Copilot Studio (see COPILOT_STUDIO_SETUP.md)
# Step 5: Import output CSVs into Power BI for dashboard
```

## Output Files

| File | Description |
|------|-------------|
| `reconciliation_report.csv` | Full 3-way recon with break flags |
| `settlement_breaks.csv` | Breaks only with categorization |
| `counterparty_summary.csv` | Break stats by counterparty |
| `daily_break_trend.csv` | Daily break rate trend |
| `aging_summary.csv` | Break aging distribution |
| `daily_settlement_summary.txt` | GenAI daily summary (plain English) |
| `root_cause_analysis.txt` | GenAI root cause analysis |
| `anomaly_alerts.json` | Automated anomaly alerts |
| `copilot_knowledge_base.json` | Copilot Studio knowledge base |

## Key Metrics (Sample Run)

- **50,000+** synthetic trades processed across 3 datasets
- **82.8%** settlement match rate
- **8,593** breaks detected and categorized
- **6** break types automatically classified
- **4** anomaly detection alerts generated
- **15** counterparty risk profiles analyzed
- **$5.87B** total notional exposure from breaks identified

## Production Deployment Path

This project is designed for migration to Azure Databricks:

1. **Data Layer**: Replace CSV reads with Delta Lake table reads (`spark.read.format("delta")`)
2. **Processing**: Swap Pandas operations for PySpark DataFrames (equivalents documented in code)
3. **Storage**: Write outputs to Delta Lake tables instead of CSV
4. **GenAI Layer**: Connect to Azure OpenAI API for dynamic natural language generation
5. **Scheduling**: Set up Databricks Jobs for daily automated reconciliation runs
6. **Dashboard**: Connect Power BI directly to Databricks SQL endpoint

## Author

**Bhuvanachandriga Sivanandam**
Senior Data Analyst | 7+ Years Banking & Financial Services
[LinkedIn](https://linkedin.com/in/bhuvanachandriga) | [GitHub](https://github.com/Bhuvanachandriga-Sivanandam)
