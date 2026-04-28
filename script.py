import sqlite3
import csv
from pathlib import Path

DB_PATH = Path("data/raw/unicamp_network.db")
OUT_DIR = Path("data/raw")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

tables = ["publicacoes", "autores_brutos", "autor_publicacao", "autores_unicamp"]

for table in tables:
    cursor.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]

    out_path = OUT_DIR / f"{table}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)

    print(f"{table}: {len(rows)} rows -> {out_path}")

conn.close()