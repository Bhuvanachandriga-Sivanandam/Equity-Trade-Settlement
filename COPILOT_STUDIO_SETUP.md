# Microsoft Copilot Studio — Setup Guide
## Equity Trade Settlement Agent

### Step 1: Create the Agent
1. Go to **copilotstudio.microsoft.com**
2. Click **"Create"** → **"New Agent"**
3. Name it: **"Equity Trade Settlement Assistant"**
4. Description: *"Operations assistant for querying settlement break data, match rates, counterparty exceptions, and anomaly alerts."*

### Step 2: Upload Knowledge Base
1. In your agent, go to **"Knowledge"** tab
2. Click **"Add Knowledge"** → **"Files"**
3. Upload these files from the `/insights/` folder:
   - `copilot_knowledge_base.json`
   - `daily_settlement_summary.txt`
   - `root_cause_analysis.txt`
   - `anomaly_alerts.txt`
4. Also upload from `/output/`:
   - `counterparty_summary.csv`
   - `daily_break_trend.csv`
   - `aging_summary.csv`

### Step 3: Configure Topics (Conversation Flows)

**Topic 1: Daily Summary**
- Trigger phrases: "daily summary", "today's report", "settlement overview", "how are we doing"
- Response: Pull from `daily_settlement_summary.txt`

**Topic 2: Counterparty Breaks**
- Trigger phrases: "counterparty breaks", "which counterparty", "worst counterparty", "break by counterparty"
- Response: Pull from `counterparty_summary.csv` knowledge

**Topic 3: Break Types**
- Trigger phrases: "break types", "what types of breaks", "most common breaks", "quantity mismatch"
- Response: Pull from knowledge base break_types section

**Topic 4: Anomaly Alerts**
- Trigger phrases: "any alerts", "anomalies", "red flags", "what should I worry about"
- Response: Pull from `anomaly_alerts.txt`

**Topic 5: Match Rate**
- Trigger phrases: "match rate", "settlement rate", "how many matched", "success rate"
- Response: Pull from knowledge base overview section

**Topic 6: Unmatched Trades**
- Trigger phrases: "unmatched trades", "failed trades", "show me breaks over", "high value breaks"
- Response: Pull from knowledge base with filtered data

### Step 4: Test the Agent
Try these natural language queries:
- "What is today's settlement match rate?"
- "Which counterparty has the most fails this week?"
- "Show me all unmatched trades over $1M"
- "What are the main causes of settlement breaks?"
- "Are there any anomalies I should know about?"
- "Give me the aging distribution of open breaks"

### Step 5: Publish & Screenshot
1. Click **"Publish"** in top right
2. Test in the chat window
3. **Take screenshots** of 3-4 queries for your portfolio
4. Save screenshots to add to your Power BI dashboard / GitHub README

### Interview Talking Point
> "I built a Microsoft Copilot Studio agent that lets operations managers 
> query settlement break data in natural language — instead of writing SQL, 
> they just ask 'which counterparty has the most fails this week?' The agent 
> pulls from our reconciliation engine's output and returns real-time insights 
> with recommended actions."
