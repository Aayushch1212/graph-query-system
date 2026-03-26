# Graph Query System — SAP Order-to-Cash

A context graph system with an LLM-powered natural language query interface built on the SAP Order-to-Cash dataset.

## Live Demo

| | Link |
|---|---|
| **Frontend** | https://graph-query-system-git-main-aayushch1212s-projects.vercel.app/ |
| **Backend API** | https://graph-query-system-3qit.onrender.com |
| **API Docs** | https://graph-query-system-3qit.onrender.com/docs |

---

## Architecture

The system is split into three layers:

**Backend (FastAPI + SQLite)**  
I chose FastAPI for its async support and automatic OpenAPI docs, which made iterating on endpoints fast. The SAP O2C JSONL dataset is ingested via a custom `ingest.py` script that normalizes 19 tables into a single SQLite database. SQLite was chosen deliberately — zero infrastructure overhead for a dataset of this size, and the full schema fits comfortably within a single file that can be committed to the repo and deployed instantly.

**Frontend (React + D3.js)**  
Built with React 18 and D3's force-directed graph layout. The graph renders nodes typed by entity (Customer, SalesOrder, Delivery, Invoice, Payment, Product) with color-coded legends and edge labels showing relationships. Clicking a node fetches and displays its full metadata from the backend.

**LLM Integration (Gemini / Groq / OpenRouter)**  
The chat interface sends natural language queries to the backend, which injects the full live schema into the system prompt and asks the LLM to generate a SQLite SQL query wrapped in `<SQL>...</SQL>` tags. The backend extracts and executes the query, then returns both the LLM's explanation and the actual data table. The system falls back across providers in order: Gemini → Groq → OpenRouter → rule-based.

---

## Database Choice

SQLite was chosen for the following reasons:

- Zero-dependency deployment — the DB file ships with the repo
- The dataset (19 tables, ~22K rows total) fits well within SQLite's performance envelope
- Schema introspection via `PRAGMA table_info()` works identically to Postgres
- Switching to Postgres in production requires changing one line (`DB_PATH` → connection string)

Three materialized views were created on top of the raw tables to support common analytical queries efficiently:
- `v_o2c_flow` — full Sales Order → Delivery → Billing → Journal pipeline
- `v_broken_flows` — orders with missing steps in the O2C chain
- `v_product_billing_counts` — products ranked by billing document frequency

---

## Graph Model

| Node Type | Source Table | Key Field |
|---|---|---|
| Customer | business_partners | businesspartner |
| SalesOrder | sales_order_headers | salesorder |
| Delivery | outbound_delivery_headers | deliverydocument |
| Invoice | billing_document_headers | billingdocument |
| Payment | journal_entry_items_ar | accountingdocument |
| Product | products | product |

**Edges:**
- Customer → SalesOrder (`PLACED`)
- SalesOrder → Delivery (`DELIVERED_VIA`, via delivery items)
- Delivery → Invoice (`INVOICED`, via billing items)
- Invoice → Payment (`JOURNALIZED`, via accounting document join)
- SalesOrder → Product (`CONTAINS`, via order items)

---

## LLM Prompting Strategy

Each request builds a system prompt that includes:

1. **Live schema** — every table and view with column names, types, and row counts pulled fresh from SQLite at request time
2. **Relationship map** — explicit JOIN paths between tables so the LLM doesn't have to infer foreign keys
3. **Helper view hints** — the three views are described with their use cases so the LLM prefers them for common queries
4. **Output format instruction** — the LLM must wrap SQL in `<SQL>...</SQL>` tags; the backend uses regex extraction to isolate and execute it
5. **Row limit** — responses are capped at 15 rows by default unless the user requests more

This approach keeps answers fully grounded — no answer is returned without an actual SQL execution result backing it.

---

## Guardrails

Two layers of protection against off-topic queries:

**LLM-level:** The system prompt explicitly instructs the model to respond with a fixed restriction message for any query outside the SAP O2C domain, with concrete examples (general knowledge, creative writing, coding questions).

**Rule-based layer:** Before the LLM is called, a keyword check runs against a domain vocabulary list. Queries with no domain keywords are rejected immediately without consuming an LLM call.

Off-topic response:
> "This system is designed to answer questions related to the provided dataset only."

---

## Example Queries

- `Which products are associated with the highest number of billing documents?`
- `Trace the full flow of the first billing document`
- `Which sales orders have broken or incomplete flows?`
- `Show me orders delivered but not invoiced`
- `What is the total payment amount received?`
- `Top 5 customers by number of orders`

---

## Running Locally

**Backend:**
```bash
cd backend
pip install -r requirements.txt
python ingest.py --data_dir /path/to/sap-o2c-data
uvicorn main:app --reload --port 8000 --host 0.0.0.0
```

**Frontend:**
```bash
cd frontend
npm install
REACT_APP_API_URL=http://localhost:8000 npm start
```

**Environment variables:**
```
GEMINI_API_KEY=your_key   # preferred
GROQ_API_KEY=your_key     # fallback
OPENROUTER_API_KEY=your_key  # fallback
```

---

## Tech Stack

- Python 3.11, FastAPI, SQLite, httpx
- React 18, D3.js v7, Vite
- Google Gemini 1.5 Flash (primary LLM)
- Deployed on Render (backend) + Vercel (frontend)
