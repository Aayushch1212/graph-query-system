import sqlite3, os

db_path = "o2c.db"
print(f"DB exists: {os.path.exists(db_path)}")
print(f"DB size: {os.path.getsize(db_path) if os.path.exists(db_path) else 'N/A'} bytes")

conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cur.fetchall()
print(f"\nTables found: {len(tables)}")
for t in tables:
    cur.execute(f"SELECT COUNT(*) FROM [{t[0]}]")
    count = cur.fetchone()[0]
    print(f"  {t[0]}: {count} rows")
conn.close()
