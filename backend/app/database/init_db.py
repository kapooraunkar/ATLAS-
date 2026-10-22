
from app.database.connection import get_connection


def initialize_database():
    connection = get_connection()

    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                duration_ms REAL NOT NULL,
                cpu_percent REAL NOT NULL,
                memory_percent REAL NOT NULL,
                source TEXT NOT NULL DEFAULT 'atlas'
            )
        """)

        connection.execute("""
            CREATE TABLE IF NOT EXISTS telemetry_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL,
                method TEXT NOT NULL,
                path TEXT NOT NULL,
                status_code INTEGER NOT NULL,
                duration_ms REAL NOT NULL,
                cpu_percent REAL NOT NULL,
                memory_percent REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0
            )
        """)

        # Migrate existing databases that don't have the source column.
        columns = connection.execute(
            "PRAGMA table_info(telemetry)"
        ).fetchall()

        column_names = {column["name"] for column in columns}

        if "source" not in column_names:
            connection.execute("""
                ALTER TABLE telemetry
                ADD COLUMN source TEXT NOT NULL DEFAULT 'atlas'
            """)

        connection.commit()

    finally:
        connection.close()
