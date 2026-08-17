from app.database.connection import get_connection


def initialize_database():
    connection = get_connection()

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS telemetry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,   -- this is database's identity
            request_id TEXT NOT NULL,   -- this is atlas' request identity
            method TEXT NOT NULL,
            path TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            duration_ms REAL NOT NULL,
            cpu_percent REAL NOT NULL,
            memory_percent REAL NOT NULL
        )
        """
    )

    connection.execute(
    """
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
    """
)

    connection.commit()
    connection.close()