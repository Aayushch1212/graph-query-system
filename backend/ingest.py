"""
ingest.py  —  SAP Order-to-Cash Dataset Ingestion
===================================================
Each folder contains multiple JSONL part files (newline-delimited JSON).
This script reads ALL part files from ALL 19 folders and loads them into SQLite.

Usage:
    python ingest.py --data_dir "C:/Users/aayus/Downloads/sap-order-to-cash-dataset/sap-o2c-data"
    python ingest.py --data_dir /path/to/sap-o2c-data
"""

import sqlite3
import pandas as pd
import os
import glob
import json
import argparse
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "../data/business.db")

# Exact folder name → SQLite table name
FOLDER_TABLE_MAP = {
    "billing_document_cancellations":           "billing_document_cancellations",
    "billing_document_headers":                 "billing_document_headers",
    "billing_document_items":                   "billing_document_items",
    "business_partner_addresses":               "business_partner_addresses",
    "business_partners":                        "business_partners",
    "customer_company_assignments":             "customer_company_assignments",
    "customer_sales_area_assignments":          "customer_sales_area_assignments",
    "journal_entry_items_accounts_receivable":  "journal_entry_items_ar",
    "outbound_delivery_headers":                "outbound_delivery_headers",
    "outbound_delivery_items":                  "outbound_delivery_items",
    "payments_accounts_receivable":             "payments_ar",
    "plants":                                   "plants",
    "product_descriptions":                     "product_descriptions",
    "product_plants":                           "product_plants",
    "product_storage_locations":                "product_storage_locations",
    "products":                                 "products",
    "sales_order_headers":                      "sales_order_headers",
    "sales_order_items":                        "sales_order_items",
    "sales_order_schedule_lines":               "sales_order_schedule_lines",
}


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        c.strip().lower().replace(" ", "_").replace("-", "_").replace(".", "_")
        for c in df.columns
    ]
    return df


def read_jsonl_files(folder_path: str):
    """Read ALL JSONL/JSON/CSV/Parquet part files from a folder and concatenate."""
    all_files = []
    for pat in ["*.jsonl", "*.json", "*.csv", "*.parquet"]:
        all_files.extend(glob.glob(os.path.join(folder_path, pat)))

    if not all_files:
        return None

    chunks = []
    for fpath in sorted(all_files):
        ext = os.path.splitext(fpath)[1].lower()
        try:
            if ext in (".jsonl", ".json"):
                # Parse newline-delimited JSON (JSONL)
                records = []
                with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                records.append(json.loads(line))
                            except json.JSONDecodeError:
                                pass
                if records:
                    chunks.append(pd.DataFrame(records))
                else:
                    # Fallback: try as a single JSON array
                    try:
                        df_j = pd.read_json(fpath, dtype=str)
                        chunks.append(df_j)
                    except Exception:
                        pass
            elif ext == ".csv":
                for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                    try:
                        chunks.append(pd.read_csv(fpath, encoding=enc, dtype=str, low_memory=False))
                        break
                    except Exception:
                        continue
            elif ext == ".parquet":
                chunks.append(pd.read_parquet(fpath))
        except Exception as e:
            print(f"    ⚠ Could not read {os.path.basename(fpath)}: {e}")

    if not chunks:
        return None

    combined = pd.concat(chunks, ignore_index=True)
    combined = normalise_columns(combined)
    combined = combined.dropna(how="all")

    # Flatten columns that contain dicts/lists (nested JSON) into strings
    # so drop_duplicates() doesn't crash with "unhashable type: dict"
    for col in combined.columns:
        if combined[col].apply(lambda x: isinstance(x, (dict, list))).any():
            combined[col] = combined[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
            )

    combined = combined.drop_duplicates()
    return combined


def load_to_sqlite(df: pd.DataFrame, table: str, conn: sqlite3.Connection):
    df.to_sql(table, conn, if_exists="replace", index=False)
    preview = list(df.columns[:6])
    suffix = "..." if len(df.columns) > 6 else ""
    print(f"  ✓  {table:<52}  {len(df):>8,} rows   cols: {preview}{suffix}")


def discover_schema(conn: sqlite3.Connection) -> dict:
    """Return {table_name: [col1, col2, ...]} for all tables."""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    schema = {}
    for (tname,) in cur.fetchall():
        cur.execute(f"PRAGMA table_info({tname})")
        schema[tname] = [r[1] for r in cur.fetchall()]
    return schema


def find_col(cols: list, *candidates):
    for c in candidates:
        if c in cols:
            return c
    return None


def create_views(conn: sqlite3.Connection, schema: dict):
    """Create helper views using dynamically discovered column names."""
    cur = conn.cursor()

    def col(table, *candidates):
        return find_col(schema.get(table, []), *candidates)

    # ── Key column discovery ─────────────────────────────────────────────────
    soh_id   = col("sales_order_headers",    "sales_order_id","salesorderid","vbeln","id","order_id")
    soh_cust = col("sales_order_headers",    "customer_id","sold_to_party","kunnr","soldtoparty","soldto")
    soh_date = col("sales_order_headers",    "creation_date","order_date","erdat","createdat","created_at")
    soh_val  = col("sales_order_headers",    "net_value","netwr","total_value","ordervalue","net_amount")
    soh_cur  = col("sales_order_headers",    "currency","waerk","curr","doc_currency")

    odh_del  = col("outbound_delivery_headers","delivery_id","deliveryid","vbeln","id","delivery_doc_id")
    odh_so   = col("outbound_delivery_headers","sales_order_id","vgbel","reference_doc","ref_order","so_id")
    odh_date = col("outbound_delivery_headers","delivery_date","lfdat","planned_gi_date","actual_delivery_date","ship_date")

    bdh_id   = col("billing_document_headers","billing_doc_id","billingdocid","vbeln","id","billing_id")
    bdh_del  = col("billing_document_headers","delivery_id","vgbel","ref_delivery","reference_delivery","reference_doc")
    bdh_so   = col("billing_document_headers","sales_order_id","so_id","ref_order")
    bdh_date = col("billing_document_headers","billing_date","fkdat","invoice_date","createdat","posting_date")
    bdh_val  = col("billing_document_headers","net_value","netwr","total_value","billing_amount")

    par_id   = col("payments_ar","payment_id","paymentid","belnr","id","clearing_doc")
    par_bid  = col("payments_ar","billing_doc_id","billingdocid","vbeln","ref_doc","document_id","clearing_ref")
    par_date = col("payments_ar","payment_date","budat","posting_date","clearing_date")
    par_amt  = col("payments_ar","amount","wrbtr","payment_amount","dmbtr","net_amount")

    soi_so   = col("sales_order_items","sales_order_id","vbeln","order_id","so_id")
    soi_pid  = col("sales_order_items","product_id","matnr","material","material_id","product")
    odi_so   = col("outbound_delivery_items","sales_order_id","vgbel","ref_order","so_id")
    odi_del  = col("outbound_delivery_items","delivery_id","vbeln","delivery_doc")
    bdi_del  = col("billing_document_items","delivery_id","vgbel","ref_delivery")
    bdi_bid  = col("billing_document_items","billing_doc_id","vbeln","billing_id")
    pd_id    = col("product_descriptions","product_id","matnr","material","id","product")
    pd_desc  = col("product_descriptions","description","maktx","product_name","name","text","product_desc")

    # ── View 1: Full O2C flow ────────────────────────────────────────────────
    if soh_id and odh_so and odh_del and bdh_del and bdh_id:
        join_pay = f"LEFT JOIN payments_ar par ON bdh.{bdh_id} = par.{par_bid}" if (par_id and par_bid) else ""
        cur.executescript(f"""
DROP VIEW IF EXISTS v_o2c_flow;
CREATE VIEW v_o2c_flow AS
SELECT
    soh.{soh_id}                                       AS sales_order_id,
    {'soh.' + soh_cust + ' AS customer_id,'            if soh_cust else 'NULL AS customer_id,'}
    {'soh.' + soh_date + ' AS order_date,'             if soh_date else 'NULL AS order_date,'}
    {'soh.' + soh_val  + ' AS order_value,'            if soh_val  else 'NULL AS order_value,'}
    {'soh.' + soh_cur  + ' AS currency,'               if soh_cur  else 'NULL AS currency,'}
    odh.{odh_del}                                      AS delivery_id,
    {'odh.' + odh_date + ' AS delivery_date,'          if odh_date else 'NULL AS delivery_date,'}
    bdh.{bdh_id}                                       AS billing_doc_id,
    {'bdh.' + bdh_date + ' AS billing_date,'           if bdh_date else 'NULL AS billing_date,'}
    {'bdh.' + bdh_val  + ' AS billing_value,'          if bdh_val  else 'NULL AS billing_value,'}
    {'par.' + par_id   + ' AS payment_id,'             if par_id   else 'NULL AS payment_id,'}
    {'par.' + par_date + ' AS payment_date,'           if par_date else 'NULL AS payment_date,'}
    {'par.' + par_amt  + ' AS payment_amount'          if par_amt  else 'NULL AS payment_amount'}
FROM sales_order_headers soh
LEFT JOIN outbound_delivery_headers odh ON soh.{soh_id}  = odh.{odh_so}
LEFT JOIN billing_document_headers  bdh ON odh.{odh_del} = bdh.{bdh_del}
{join_pay};
""")
        print("  ✓  view: v_o2c_flow")

    # ── View 2: Broken flows ─────────────────────────────────────────────────
    if soh_id and odh_so and odh_del and bdh_del and bdh_id:
        join_pay2 = f"LEFT JOIN payments_ar par ON bdh.{bdh_id} = par.{par_bid}" if (par_id and par_bid) else ""
        pay_where = f"OR par.{par_id} IS NULL" if par_id else ""
        pay_status = f"CASE WHEN par.{par_id} IS NULL THEN 'Unpaid' ELSE 'Paid' END AS payment_status" if par_id else "'Unknown' AS payment_status"
        cur.executescript(f"""
DROP VIEW IF EXISTS v_broken_flows;
CREATE VIEW v_broken_flows AS
SELECT
    soh.{soh_id}   AS sales_order_id,
    {'soh.' + soh_cust + ' AS customer_id,' if soh_cust else 'NULL AS customer_id,'}
    {'soh.' + soh_date + ' AS order_date,'  if soh_date else 'NULL AS order_date,'}
    CASE WHEN odh.{odh_del} IS NULL THEN 'Missing Delivery' ELSE 'Has Delivery' END AS delivery_status,
    CASE WHEN bdh.{bdh_id}  IS NULL THEN 'Not Billed'       ELSE 'Billed'       END AS billing_status,
    {pay_status}
FROM sales_order_headers soh
LEFT JOIN outbound_delivery_headers odh ON soh.{soh_id}  = odh.{odh_so}
LEFT JOIN billing_document_headers  bdh ON odh.{odh_del} = bdh.{bdh_del}
{join_pay2}
WHERE odh.{odh_del} IS NULL OR bdh.{bdh_id} IS NULL {pay_where};
""")
        print("  ✓  view: v_broken_flows")

    # ── View 3: Product billing counts ──────────────────────────────────────
    if pd_id and soi_pid and soi_so and odi_so and odi_del and bdi_del and bdi_bid:
        cur.executescript(f"""
DROP VIEW IF EXISTS v_product_billing_counts;
CREATE VIEW v_product_billing_counts AS
SELECT
    pd.{pd_id}   AS product_id,
    {'pd.' + pd_desc + ' AS product_name,' if pd_desc else 'pd.' + pd_id + ' AS product_name,'}
    COUNT(DISTINCT bdi.{bdi_bid}) AS billing_doc_count
FROM product_descriptions pd
LEFT JOIN sales_order_items      soi ON pd.{pd_id}   = soi.{soi_pid}
LEFT JOIN outbound_delivery_items odi ON soi.{soi_so} = odi.{odi_so}
LEFT JOIN billing_document_items bdi ON odi.{odi_del} = bdi.{bdi_del}
GROUP BY pd.{pd_id} {', pd.' + pd_desc if pd_desc else ''}
ORDER BY billing_doc_count DESC;
""")
        print("  ✓  view: v_product_billing_counts")

    conn.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_dir",
        default=os.path.join(os.path.dirname(__file__), "../data/sap-o2c-data"),
        help="Path to the sap-o2c-data folder containing the 19 sub-folders"
    )
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    if not os.path.isdir(data_dir):
        print(f"\n❌  Directory not found: {data_dir}")
        print("    Usage: python ingest.py --data_dir /path/to/sap-o2c-data")
        sys.exit(1)

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    print(f"\n📂  Source : {data_dir}")
    print(f"🗄   Target : {DB_PATH}\n")

    conn = sqlite3.connect(DB_PATH)
    loaded = 0

    for folder_name, table_name in FOLDER_TABLE_MAP.items():
        folder_path = os.path.join(data_dir, folder_name)
        if not os.path.isdir(folder_path):
            print(f"  ⚠  Missing folder (skipping): {folder_name}")
            continue

        df = read_jsonl_files(folder_path)
        if df is None or df.empty:
            print(f"  ⚠  No readable files in : {folder_name}")
            continue

        load_to_sqlite(df, table_name, conn)
        loaded += 1

    print(f"\n📊  Loaded {loaded}/{len(FOLDER_TABLE_MAP)} tables.")
    print("\n🔧  Building helper views...")
    schema = discover_schema(conn)
    create_views(conn, schema)

    print("\n📋  Schema summary:")
    for tname, cols in schema.items():
        print(f"  {tname}: {cols[:8]}{'...' if len(cols) > 8 else ''}")

    conn.close()
    print(f"\n✅  Done!  →  {DB_PATH}")
    print("    Next: uvicorn main:app --reload --port 8000\n")


if __name__ == "__main__":
    main()
