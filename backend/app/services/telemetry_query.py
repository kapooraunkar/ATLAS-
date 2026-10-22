from app.database.connection import get_connection

def get_telemetry_records(source: str | None = None, limit: int = 100):
    connection = get_connection()

    try:
        if source:
            rows = connection.execute(
                """
                SELECT
                    id, request_id, method, path, status_code,
                    duration_ms, cpu_percent, memory_percent, source
                FROM telemetry
                WHERE source = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (source, limit),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT
                    id, request_id, method, path, status_code,
                    duration_ms, cpu_percent, memory_percent, source
                FROM telemetry
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [dict(row) for row in rows]

    finally:
        connection.close()
