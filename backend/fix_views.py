import sqlite3

conn = sqlite3.connect("o2c.db")
cur = conn.cursor()

# Check actual column names
cur.execute("PRAGMA table_info(billing_document_items)")
bdi_cols = [r[1] for r in cur.fetchall()]
print("billing_document_items cols:", bdi_cols)

cur.execute("PRAGMA table_info(products)")
prod_cols = [r[1] for r in cur.fetchall()]
print("products cols:", prod_cols)

cur.execute("PRAGMA table_info(sales_order_headers)")
soh_cols = [r[1] for r in cur.fetchall()]
print("sales_order_headers cols:", soh_cols)

cur.execute("PRAGMA table_info(outbound_delivery_headers)")
odh_cols = [r[1] for r in cur.fetchall()]
print("outbound_delivery_headers cols:", odh_cols)

cur.execute("PRAGMA table_info(billing_document_headers)")
bdh_cols = [r[1] for r in cur.fetchall()]
print("billing_document_headers cols:", bdh_cols)

cur.execute("PRAGMA table_info(journal_entry_items_ar)")
jei_cols = [r[1] for r in cur.fetchall()]
print("journal_entry_items_ar cols:", jei_cols)

conn.commit()

# Find material column in billing_document_items
mat_col = next((c for c in bdi_cols if 'material' in c.lower() or 'product' in c.lower()), bdi_cols[0])
bill_doc_col = next((c for c in bdi_cols if 'billingdoc' in c.lower() or 'billing_doc' in c.lower() or 'vbeln' in c.lower()), bdi_cols[0])
print(f"\nUsing: material={mat_col}, billing_doc={bill_doc_col}")

# Find product id column
prod_id_col = next((c for c in prod_cols if 'product' in c.lower() or 'material' in c.lower()), prod_cols[0])
prod_desc_col = next((c for c in prod_cols if 'desc' in c.lower() or 'name' in c.lower()), prod_id_col)
print(f"Using: product_id={prod_id_col}, desc={prod_desc_col}")

# Drop and recreate views
views = [
    "v_product_billing_counts",
    "v_o2c_flow",
    "v_broken_flows"
]
for v in views:
    cur.execute(f"DROP VIEW IF EXISTS [{v}]")

# v_product_billing_counts
cur.execute(f"""
CREATE VIEW v_product_billing_counts AS
SELECT 
    bdi.{mat_col} AS material,
    p.{prod_desc_col} AS product_description,
    COUNT(DISTINCT bdi.{bill_doc_col}) AS billing_doc_count
FROM billing_document_items bdi
LEFT JOIN products p ON p.{prod_id_col} = bdi.{mat_col}
GROUP BY bdi.{mat_col}
ORDER BY billing_doc_count DESC
""")
print("✅ Created v_product_billing_counts")

# Find key columns for o2c flow
so_col = next((c for c in soh_cols if 'salesorder' in c.lower() and 'item' not in c.lower() and 'type' not in c.lower()), soh_cols[0])
cust_col = next((c for c in soh_cols if 'customer' in c.lower() or 'soldto' in c.lower() or 'partner' in c.lower()), soh_cols[0])

del_col = next((c for c in odh_cols if 'delivery' in c.lower() and 'item' not in c.lower()), odh_cols[0])
del_so_col = next((c for c in odh_cols if 'salesorder' in c.lower() or 'order' in c.lower()), odh_cols[0] if len(odh_cols) < 2 else odh_cols[1])

bdh_col = next((c for c in bdh_cols if 'billingdoc' in c.lower() or 'billing' in c.lower()), bdh_cols[0])
bdh_del_col = next((c for c in bdh_cols if 'delivery' in c.lower() or 'reference' in c.lower()), bdh_cols[0] if len(bdh_cols) < 2 else bdh_cols[1])

je_col = next((c for c in jei_cols if 'journal' in c.lower() or 'entry' in c.lower() or 'document' in c.lower()), jei_cols[0])
je_ref_col = next((c for c in jei_cols if 'reference' in c.lower() or 'billing' in c.lower()), jei_cols[0] if len(jei_cols) < 2 else jei_cols[1])

cur.execute(f"""
CREATE VIEW v_o2c_flow AS
SELECT
    so.{so_col} AS sales_order,
    so.{cust_col} AS customer,
    d.{del_col} AS delivery,
    b.{bdh_col} AS billing_doc,
    j.{je_col} AS journal_entry
FROM sales_order_headers so
LEFT JOIN outbound_delivery_headers d ON d.{del_so_col} = so.{so_col}
LEFT JOIN billing_document_headers b ON b.{bdh_del_col} = d.{del_col}
LEFT JOIN journal_entry_items_ar j ON j.{je_ref_col} = b.{bdh_col}
""")
print("✅ Created v_o2c_flow")

cur.execute(f"""
CREATE VIEW v_broken_flows AS
SELECT
    so.{so_col} AS sales_order,
    so.{cust_col} AS customer,
    CASE 
        WHEN d.{del_col} IS NULL THEN 'No Delivery'
        WHEN b.{bdh_col} IS NULL THEN 'Delivered but not Billed'
        WHEN j.{je_col} IS NULL THEN 'Billed but no Journal Entry'
        ELSE 'Complete'
    END AS flow_status
FROM sales_order_headers so
LEFT JOIN outbound_delivery_headers d ON d.{del_so_col} = so.{so_col}
LEFT JOIN billing_document_headers b ON b.{bdh_del_col} = d.{del_col}
LEFT JOIN journal_entry_items_ar j ON j.{je_ref_col} = b.{bdh_col}
WHERE d.{del_col} IS NULL OR b.{bdh_col} IS NULL OR j.{je_col} IS NULL
""")
print("✅ Created v_broken_flows")

conn.commit()
conn.close()
print("\n✅ All views created successfully!")
