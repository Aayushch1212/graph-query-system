"""
Run this from the backend/ folder to patch main.py with correct column names.
Usage: python fix_graph.py
"""
import re

NEW_GRAPH = '''@app.get("/api/graph")
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

    # Deliveries — no direct SO link in outbound_delivery_headers, link via items
    for r in conn.execute("SELECT deliverydocument FROM outbound_delivery_headers LIMIT 80").fetchall():
        did = r[0]
        add_node(f"del_{did}", f"DEL-{did}", "Delivery", {"delivery_id": did})

    # Sales order items -> link SO to delivery via delivery items
    if conn.execute("SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'outbound_delivery_items\'").fetchone():
        cols = [r[1] for r in conn.execute("PRAGMA table_info(outbound_delivery_items)").fetchall()]
        so_c = next((c for c in cols if \'salesorder\' in c.lower()), None)
        del_c = next((c for c in cols if \'deliverydocument\' in c.lower() or \'delivery\' in c.lower() and \'item\' not in c.lower()), None)
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
    if conn.execute("SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'billing_document_items\'").fetchone():
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
'''

with open("main.py", "r") as f:
    content = f.read()

# Replace the get_graph function
new_content = re.sub(
    r'@app\.get\("/api/graph"\)\ndef get_graph\(\):.*?(?=\n# ── Stats|@app\.get\("/api/stats"\))',
    NEW_GRAPH + '\n\n',
    content,
    flags=re.DOTALL
)

with open("main.py", "w") as f:
    f.write(new_content)

print("✅ main.py patched with correct column names!")
print("Uvicorn will auto-reload. Refresh localhost:3000 to see the full graph.")
