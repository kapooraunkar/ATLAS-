import sqlite3
from pathlib import Path

DATABASE_PATH = Path(r"C:\ATLAS\backend\data\atlas.db")

print("Database exists:", DATABASE_PATH.exists())

connection = sqlite3.connect(DATABASE_PATH)
connection.row_factory = sqlite3.Row

rows = connection.execute(
    "SELECT * FROM telemetry ORDER BY rowid DESC LIMIT 5"
).fetchall()

print(f"\nFound {len(rows)} telemetry rows:\n")

for row in rows:
    print(dict(row))

connection.close()