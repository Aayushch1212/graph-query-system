"""
main.py  —  FastAPI backend for Graph Query System
SAP Order-to-Cash dataset (JSONL ingested via ingest.py)
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import sqlite3
import json
import os
import re

app = FastAPI(title="SAP O2C Graph Query System")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
        allow_credentials=True,

    allow_headers=["*"],
)

# DB_PATH = os.path.join(os.path.dirname(__file__), "../data/business.db")
DB_PATH = os.path.join(os.path.dirname(__file__), "o2c.db")


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None


def get_columns(conn, table: str) -> list:
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table})")
        return [r[1] for r in cur.fetchall()]
    except Exception:
        return []


def find_col(cols: list, *candidates) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


def first_rows(conn, table: str, n: int = 2) -> list:
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table} LIMIT {n}")
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


# ── Schema introspection (used for LLM prompt + graph) ───────────────────────

def get_full_schema() -> str:
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view') ORDER BY name")
    objects = [r[0] for r in cur.fetchall()]
    parts = []
    for obj in objects:
        cur.execute(f"PRAGMA table_info({obj})")
        cols = [f"{r[1]}({r[2] or 'TEXT'})" for r in cur.fetchall()]
        # Add row count for tables
        try:
            cur.execute(f"SELECT COUNT(*) FROM {obj}")
            cnt = cur.fetchone()[0]
            parts.append(f"{'VIEW' if obj.startswith('v_') else 'TABLE'} {obj} [{cnt:,} rows]: {', '.join(cols)}")
        except Exception:
            parts.append(f"TABLE {obj}: {', '.join(cols)}")
    conn.close()
    return "\n".join(parts)


# ── Graph construction ────────────────────────────────────────────────────────

@app.get("/api/graph")
def get_graph():
    conn = get_db()
    nodes = []
    edges = []
    seen_nodes = set()

    def add_node(nid, label, ntype, props=None):
        if nid and nid not in seen_nodes:
            seen_nodes.add(nid)
            nodes.append({"id": nid, "label": str(label)[:30], "type": ntype, "properties": props or {}})

    # Customers
    for r in conn.execute("SELECT businesspartner, businesspartnerfullname FROM business_partners LIMIT 40").fetchall():
        add_node(f"bp_{r[0]}", r[1] or f"BP-{r[0]}", "Customer", {"id": r[0]})

    # Sales Orders + Customer->Order edges
    for r in conn.execute("SELECT salesorder, soldtoparty, totalnetamount FROM sales_order_headers LIMIT 80").fetchall():
        so, cust, val = r[0], r[1], r[2]
        add_node(f"so_{so}", f"SO-{so}", "SalesOrder", {"order_id": so, "value": val})
        if cust:
            add_node(f"bp_{cust}", f"Customer {cust}", "Customer", {"id": cust})
            edges.append({"source": f"bp_{cust}", "target": f"so_{so}", "label": "PLACED"})

    # Deliveries � no direct SO link in outbound_delivery_headers, link via items
    for r in conn.execute("SELECT deliverydocument FROM outbound_delivery_headers LIMIT 80").fetchall():
        did = r[0]
        add_node(f"del_{did}", f"DEL-{did}", "Delivery", {"delivery_id": did})

    # Sales order items -> link SO to delivery via delivery items
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='outbound_delivery_items'").fetchone():
        cols = [r[1] for r in conn.execute("PRAGMA table_info(outbound_delivery_items)").fetchall()]
        so_c = next((c for c in cols if 'salesorder' in c.lower()), None)
        del_c = next((c for c in cols if 'deliverydocument' in c.lower() or 'delivery' in c.lower() and 'item' not in c.lower()), None)
        if so_c and del_c:
            for r in conn.execute(f"SELECT {so_c}, {del_c} FROM outbound_delivery_items LIMIT 100").fetchall():
                so_id, del_id = r[0], r[1]
                if so_id and del_id:
                    add_node(f"so_{so_id}", f"SO-{so_id}", "SalesOrder", {"order_id": so_id})
                    add_node(f"del_{del_id}", f"DEL-{del_id}", "Delivery", {"delivery_id": del_id})
                    ek = f"so_{so_id}->del_{del_id}"
                    if ek not in seen_nodes:
                        seen_nodes.add(ek)
                        edges.append({"source": f"so_{so_id}", "target": f"del_{del_id}", "label": "DELIVERED_VIA"})

    # Billing docs + link to customer (soldtoparty)
    for r in conn.execute("SELECT billingdocument, soldtoparty, totalnetamount FROM billing_document_headers LIMIT 80").fetchall():
        bid, cust, val = r[0], r[1], r[2]
        add_node(f"inv_{bid}", f"BILL-{bid}", "Invoice", {"billing_doc_id": bid, "value": val})

    # Link billing -> delivery via billing items (referencesddocument = delivery)
    if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='billing_document_items'").fetchone():
        for r in conn.execute("SELECT billingdocument, referencesddocument FROM billing_document_items LIMIT 200").fetchall():
            bill_id, ref_doc = r[0], r[1]
            if bill_id and ref_doc:
                add_node(f"inv_{bill_id}", f"BILL-{bill_id}", "Invoice", {"billing_doc_id": bill_id})
                add_node(f"del_{ref_doc}", f"DEL-{ref_doc}", "Delivery", {"delivery_id": ref_doc})
                ek = f"del_{ref_doc}->inv_{bill_id}"
                if ek not in seen_nodes:
                    seen_nodes.add(ek)
                    edges.append({"source": f"del_{ref_doc}", "target": f"inv_{bill_id}", "label": "INVOICED"})

    # Payments via journal_entry_items_ar (accountingdocument links to billing_document_headers)
    for r in conn.execute("""
        SELECT j.accountingdocument, j.companycode, j.fiscalyear, b.billingdocument
        FROM journal_entry_items_ar j
        JOIN billing_document_headers b ON b.accountingdocument = j.accountingdocument
            AND b.companycode = j.companycode AND b.fiscalyear = j.fiscalyear
        LIMIT 80
    """).fetchall():
        je_id, _, _, bill_id = r[0], r[1], r[2], r[3]
        add_node(f"je_{je_id}", f"JE-{je_id}", "Payment", {"journal_entry_id": je_id})
        add_node(f"inv_{bill_id}", f"BILL-{bill_id}", "Invoice", {"billing_doc_id": bill_id})
        ek = f"inv_{bill_id}->je_{je_id}"
        if ek not in seen_nodes:
            seen_nodes.add(ek)
            edges.append({"source": f"inv_{bill_id}", "target": f"je_{je_id}", "label": "JOURNALIZED"})

    # Products
    for r in conn.execute("SELECT product FROM products LIMIT 40").fetchall():
        pid = r[0]
        add_node(f"prod_{pid}", f"PROD-{pid}", "Product", {"product_id": pid})

    # SO items -> Products
    for r in conn.execute("SELECT salesorder, material FROM sales_order_items LIMIT 120").fetchall():
        so_id, prod_id = r[0], r[1]
        if so_id and prod_id:
            add_node(f"so_{so_id}", f"SO-{so_id}", "SalesOrder", {"order_id": so_id})
            add_node(f"prod_{prod_id}", f"PROD-{prod_id}", "Product", {"product_id": prod_id})
            ek = f"so_{so_id}->prod_{prod_id}"
            if ek not in seen_nodes:
                seen_nodes.add(ek)
                edges.append({"source": f"so_{so_id}", "target": f"prod_{prod_id}", "label": "CONTAINS"})

    conn.close()
    return {"nodes": nodes, "edges": edges}


@app.get("/api/stats")
def get_stats():
    conn = get_db()
    cur = conn.cursor()
    tables = {
    "business_partners":        "customers",
    "sales_order_headers":      "sales_orders",
    "outbound_delivery_headers":"deliveries",
    "billing_document_headers": "invoices",
    "payments_ar":              "payments",
    "products":                 "products",
    "sales_order_items":        "order_items",
    "journal_entry_items_ar":   "journal_entries",
    }
    stats = {}
    for tbl, label in tables.items():
        try:
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            stats[label] = cur.fetchone()[0]
        except Exception:
            stats[label] = 0
    conn.close()
    return stats


# ── Node detail ────────────────────────────────────────────────────────────────

@app.get("/api/node/{node_id}")
def get_node_detail(node_id: str):
    parts = node_id.split("_", 1)
    prefix, eid = parts[0], parts[1] if len(parts) > 1 else ""
    prefix_map = {
        "bp":   ("business_partners",        ["business_partner_id","id","partner_id","kunnr"]),
        "so":   ("sales_order_headers",       ["sales_order_id","vbeln","id","order_id"]),
        "del":  ("outbound_delivery_headers", ["delivery_id","vbeln","id","delivery_doc_id"]),
        "inv":  ("billing_document_headers",  ["billing_doc_id","vbeln","id","billing_id"]),
        "pay":  ("payments_ar",               ["payment_id","belnr","id","clearing_doc"]),
        "prod": ("product_descriptions",      ["product_id","matnr","id","material"]),
        "je":   ("journal_entry_items_ar",    ["journal_entry_id","belnr","id","je_id"]),
    }
    if prefix not in prefix_map:
        raise HTTPException(404, "Unknown node type")
    table, pk_candidates = prefix_map[prefix]
    conn = get_db()
    cols = get_columns(conn, table)
    pk = find_col(cols, *pk_candidates)
    if not pk:
        conn.close()
        raise HTTPException(404, f"PK not found in {table}")
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE {pk} = ?", (eid,))
    row = cur.fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Record not found")
    return dict(row)


# ── Chat / NL query ────────────────────────────────────────────────────────────

SYSTEM_PROMPT_TEMPLATE = """You are a SQL data analyst for a SAP Order-to-Cash (O2C) business database.

YOUR ONLY JOB: Answer questions about this specific database. Nothing else.

DATABASE SCHEMA (live, from SQLite):
{schema}

KEY RELATIONSHIPS:
- business_partners → sales_order_headers (customer places order)
- sales_order_headers → outbound_delivery_headers (order fulfilled via delivery)
- outbound_delivery_headers → billing_document_headers (delivery generates billing doc)
- billing_document_headers → payments_ar (billing doc gets paid)
- sales_order_items links orders to products
- journal_entry_items_ar links billing docs to accounting entries
- billing_document_cancellations tracks cancelled billing docs

HELPER VIEWS (pre-built for common queries):
- v_o2c_flow: full Sales Order → Delivery → Billing → Payment pipeline in one view
- v_broken_flows: orders missing delivery, billing, or payment
- v_product_billing_counts: products ranked by billing document count

RULES:
1. If the question is NOT about this dataset (general knowledge, coding, creative writing, weather, etc.), respond EXACTLY with:
   "This system is designed to answer questions related to the provided dataset only."
   
2. For valid data questions: write a SQLite SQL query inside <SQL>...</SQL> tags, then give a clear natural language answer.

3. Use the helper views (v_o2c_flow, v_broken_flows, v_product_billing_counts) when relevant — they are already optimised.

4. Always LIMIT to 15 rows unless the user asks for more.

5. Never invent data. Base all answers on actual SQL results.

6. For "trace" queries, use v_o2c_flow filtered by the specific ID.

7. Column names must match the schema exactly — check the schema before writing SQL.

GUARDRAIL EXAMPLES — respond with the restriction message for:
- "Who is the president of France?" → restriction message
- "Write me a poem" → restriction message
- "Explain quantum physics" → restriction message
- "What is Python?" → restriction message
- "How many sales orders exist?" → VALID, write SQL"""


class ChatRequest(BaseModel):
    message: str
    history: Optional[list] = []


@app.post("/api/chat")
async def chat(req: ChatRequest):
    schema = get_full_schema()
    system = SYSTEM_PROMPT_TEMPLATE.format(schema=schema)

    messages = []
    for h in (req.history or []):
        messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": req.message})

    # Try LLM providers in order
    llm_text = ""
    if os.environ.get("GEMINI_API_KEY"):
        llm_text = await call_gemini(system, messages, os.environ["GEMINI_API_KEY"])
    elif os.environ.get("GROQ_API_KEY"):
        llm_text = await call_groq(system, messages, os.environ["GROQ_API_KEY"])
    elif os.environ.get("OPENROUTER_API_KEY"):
        llm_text = await call_openrouter(system, messages, os.environ["OPENROUTER_API_KEY"])
    else:
        llm_text = rule_based_fallback(req.message)

    # Extract <SQL>...</SQL> and execute
    sql_result = None
    display_text = llm_text

    sql_match = re.search(r"<SQL>(.*?)</SQL>", llm_text, re.DOTALL | re.IGNORECASE)
    if sql_match:
        sql = sql_match.group(1).strip()
        sql_result = execute_sql(sql)
        display_text = llm_text[:sql_match.start()] + llm_text[sql_match.end():]

    return {
        "response": display_text.strip(),
        "sql_result": sql_result,
    }


def execute_sql(sql: str) -> dict:
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        conn.close()
        if not rows:
            return {"columns": [], "rows": [], "count": 0}
        columns = [d[0] for d in cur.description]
        return {"columns": columns, "rows": [list(r) for r in rows], "count": len(rows)}
    except Exception as e:
        return {"error": str(e), "columns": [], "rows": [], "count": 0}


# ── LLM Providers ─────────────────────────────────────────────────────────────

async def call_gemini(system: str, messages: list, api_key: str) -> str:
    import httpx
    contents = []
    for m in messages:
        contents.append({
            "role": "user" if m["role"] == "user" else "model",
            "parts": [{"text": m["content"]}]
        })
    payload = {
        "system_instruction": {"parts": [{"text": system}]},
        "contents": contents,
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 2000},
    }
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
    async with httpx.AsyncClient(timeout=40) as client:
        r = await client.post(url, json=payload)
        data = r.json()
        return data["candidates"][0]["content"]["parts"][0]["text"]


async def call_groq(system: str, messages: list, api_key: str) -> str:
    import httpx
    msgs = [{"role": "system", "content": system}] + messages
    payload = {"model": "llama3-70b-8192", "messages": msgs, "temperature": 0.1, "max_tokens": 2000}
    async with httpx.AsyncClient(timeout=40) as client:
        r = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            json=payload,
            headers={"Authorization": f"Bearer {api_key}"},
        )
        return r.json()["choices"][0]["message"]["content"]


async def call_openrouter(system: str, messages: list, api_key: str) -> str:
    import httpx
    msgs = [{"role": "system", "content": system}] + messages
    payload = {
        "model": "mistralai/mistral-7b-instruct:free",
        "messages": msgs,
        "temperature": 0.1,
    }
    async with httpx.AsyncClient(timeout=40) as client:
        r = await client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            json=payload,
            headers={
                "Authorization": f"Bearer {api_key}",
                "HTTP-Referer": "http://localhost:3000",
            },
        )
        return r.json()["choices"][0]["message"]["content"]


# ── Rule-based fallback (no LLM key) ─────────────────────────────────────────

DOMAIN_KEYWORDS = [
    "order", "delivery", "invoice", "billing", "payment", "customer", "product",
    "partner", "sales", "ship", "deliver", "billed", "paid", "flow", "broken",
    "incomplete", "trace", "journal", "plant", "material", "schedule", "cancel",
    "outbound", "revenue", "amount", "value", "count", "total", "list", "show",
    "how many", "which", "find", "get", "top", "highest", "lowest", "status",
]

def rule_based_fallback(message: str) -> str:
    msg = message.lower()
    if not any(kw in msg for kw in DOMAIN_KEYWORDS):
        return "This system is designed to answer questions related to the provided dataset only."

    if "broken" in msg or "incomplete" in msg:
        sql = "SELECT * FROM v_broken_flows LIMIT 15"
        return f"<SQL>{sql}</SQL>\nHere are the sales orders with broken or incomplete O2C flows."

    if "product" in msg and ("billing" in msg or "invoice" in msg or "highest" in msg or "most" in msg):
        sql = "SELECT * FROM v_product_billing_counts LIMIT 15"
        return f"<SQL>{sql}</SQL>\nHere are the products ranked by billing document count."

    if "trace" in msg or "flow" in msg or "full" in msg:
        sql = "SELECT * FROM v_o2c_flow LIMIT 15"
        return f"<SQL>{sql}</SQL>\nHere is the full O2C flow (Sales Order → Delivery → Billing → Payment)."

    if "cancel" in msg:
        sql = "SELECT * FROM billing_document_cancellations LIMIT 15"
        return f"<SQL>{sql}</SQL>\nHere are the billing document cancellations."

    if "payment" in msg and ("total" in msg or "sum" in msg or "amount" in msg):
        sql = "SELECT COUNT(*) as total_payments, SUM(CAST(amount AS REAL)) as total_amount FROM payments_ar"
        return f"<SQL>{sql}</SQL>\nHere is the total payment summary."

    if "customer" in msg or "partner" in msg:
        sql = "SELECT * FROM business_partners LIMIT 15"
        return f"<SQL>{sql}</SQL>\nHere are the business partners / customers."

    if "order" in msg and ("count" in msg or "how many" in msg or "total" in msg):
        sql = "SELECT COUNT(*) as total_orders FROM sales_order_headers"
        return f"<SQL>{sql}</SQL>\nHere is the total sales order count."

    if "deliver" in msg:
        sql = "SELECT * FROM outbound_delivery_headers LIMIT 15"
        return f"<SQL>{sql}</SQL>\nHere are the outbound deliveries."

    # Default: show the O2C flow
    sql = "SELECT * FROM v_o2c_flow LIMIT 10"
    return f"<SQL>{sql}</SQL>\nHere's an overview of the Order-to-Cash flow. You can ask about specific orders, deliveries, billing documents, or payments."
