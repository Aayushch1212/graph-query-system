# SAP Order-to-Cash — Graph Query System

A context graph + LLM-powered natural language query interface for the **SAP O2C dataset**.

---

## Architecture

```
React + D3.js (force graph)         FastAPI (Python)              SQLite
  GraphView (nodes/edges)    →→→   /api/graph                 19 tables from
  ChatPanel (NL queries)     →→→   /api/chat → LLM → SQL →    JSONL part files
  NodeDetail (inspector)     →→→   /api/node
  StatsBar (entity counts)   →→→   /api/stats
```

---

## Dataset

19 folders, each containing multiple JSONL part files:

| Folder | SQLite Table |
|---|---|
| billing_document_cancellations | billing_document_cancellations |
| billing_document_headers | billing_document_headers |
| billing_document_items | billing_document_items |
| business_partner_addresses | business_partner_addresses |
| business_partners | business_partners |
| customer_company_assignments | customer_company_assignments |
| customer_sales_area_assignments | customer_sales_area_assignments |
| journal_entry_items_accounts_receivable | journal_entry_items_ar |
| outbound_delivery_headers | outbound_delivery_headers |
| outbound_delivery_items | outbound_delivery_items |
| payments_accounts_receivable | payments_ar |
| plants | plants |
| product_descriptions | product_descriptions |
| product_plants | product_plants |
| product_storage_locations | product_storage_locations |
| products | products |
| sales_order_headers | sales_order_headers |
| sales_order_items | sales_order_items |
| sales_order_schedule_lines | sales_order_schedule_lines |

---

## Quick Start

### Step 1 — Ingest the dataset

```bash
cd backend
pip install -r requirements.txt

python ingest.py --data_dir "C:\Users\aayus\Downloads\sap-order-to-cash-dataset\sap-o2c-data"
```

The ingestion script:
- Reads ALL `.jsonl` part files from each folder
- Concatenates and deduplicates them
- Normalises column names (lowercase + underscores)
- Loads into SQLite at `data/business.db`
- Auto-discovers column names and builds 3 helper views

### Step 2 — Set LLM API key (pick ONE — all free tier)

```bash
# Google Gemini (recommended): https://ai.google.dev
set GEMINI_API_KEY=your_key_here       # Windows
export GEMINI_API_KEY=your_key_here    # Mac/Linux

# OR Groq: https://console.groq.com
set GROQ_API_KEY=your_key_here

# OR OpenRouter: https://openrouter.ai
set OPENROUTER_API_KEY=your_key_here
```

### Step 3 — Start backend

```bash
uvicorn main:app --reload --port 8000
```

### Step 4 — Start frontend

```bash
cd frontend
npm install
npm start
# Opens at http://localhost:3000
```

---

## Graph Model

### Nodes
| Type | Source |
|---|---|
| Customer | business_partners |
| SalesOrder | sales_order_headers |
| Delivery | outbound_delivery_headers |
| Invoice | billing_document_headers |
| Payment | payments_ar |
| Product | product_descriptions |
| JournalEntry | journal_entry_items_ar |

### Edges
| From → To | Label |
|---|---|
| Customer → SalesOrder | PLACED |
| SalesOrder → Delivery | DELIVERED_VIA |
| Delivery → Invoice | INVOICED |
| Invoice → Payment | PAID_BY |
| SalesOrder → Product | CONTAINS |
| Invoice → JournalEntry | JOURNALIZED |

---

## LLM Prompting Strategy

1. **Live schema injection** — `PRAGMA table_info()` runs at every request to inject actual column names and row counts into the system prompt. The LLM always sees the real schema.

2. **Structured SQL output** — The LLM wraps SQL in `<SQL>...</SQL>` tags. The backend extracts, executes, and returns both the natural language response and a rendered result table.

3. **Pre-built views** — Three helper views (`v_o2c_flow`, `v_broken_flows`, `v_product_billing_counts`) are created at ingest time. The LLM is told about them and uses them for complex queries.

4. **Guardrails** — The system prompt explicitly restricts answers to the dataset domain and provides the exact guardrail message for off-topic queries. A rule-based fallback also checks domain keywords before answering without an LLM key.

5. **Conversation memory** — Full message history is sent with every request for multi-turn context.

---

## Helper Views

After ingestion, these views are auto-created:

```sql
-- Full pipeline in one query
SELECT * FROM v_o2c_flow;

-- Orders with missing steps
SELECT * FROM v_broken_flows;

-- Products ranked by billing docs
SELECT * FROM v_product_billing_counts;
```

---

## Example Queries

- *"Which products have the highest number of billing documents?"*
- *"Show me orders with broken or incomplete flows"*
- *"Trace the full flow of sales order 123"*
- *"How many orders were delivered but not billed?"*
- *"What is the total amount received in payments?"*
- *"List all billing document cancellations"*
- *"Which customer has the most sales orders?"*

Off-topic queries → *"This system is designed to answer questions related to the provided dataset only."*

---

## Database Choice: SQLite

- Zero infrastructure — file-based, no server needed
- The O2C data is inherently relational; SQL is the natural query language
- `PRAGMA table_info()` gives live schema for the LLM prompt
- Trivially swappable to PostgreSQL by changing one connection string

---

## Deployment

```bash
# Backend on Railway/Render
uvicorn main:app --host 0.0.0.0 --port $PORT

# Frontend on Vercel/Netlify
REACT_APP_API_URL=https://your-backend.com npm run build
```

Or use Docker:

```bash
docker-compose up
```
