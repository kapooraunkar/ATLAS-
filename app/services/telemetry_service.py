
from app.schemas.telemetry import Telemetry
from app.database.connection import get_connection
import time


def process_telemetry(telemetry: Telemetry):
    print("\n========== TELEMETRY ==========")
    print(f"Request ID  : {telemetry.request_id}")
    print(f"Method      : {telemetry.method}")
    print(f"Path        : {telemetry.path}")
    print(f"Status Code : {telemetry.status_code}")
    print(f"Duration    : {telemetry.duration_ms:.2f} ms")
    print(f"CPU         : {telemetry.cpu_percent:.1f}%")
    print(f"Memory      : {telemetry.memory_percent:.1f}%")
    print(f"Source      : {telemetry.source}")
    print("===============================\n")

    connection = get_connection()

    try:
        connection.execute(
            """
            INSERT INTO telemetry (
                request_id,
                method,
                path,
                status_code,
                duration_ms,
                cpu_percent,
                memory_percent,
                source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                telemetry.request_id,
                telemetry.method,
                telemetry.path,
                telemetry.status_code,
                telemetry.duration_ms,
                telemetry.cpu_percent,
                telemetry.memory_percent,
                telemetry.source,
            ),
        )

        connection.commit()

    finally:
        connection.close()


def check_persistence_health():
    start_time = time.monotonic()
    connection = None

    try:
        connection = get_connection()
        connection.execute("SELECT 1")

        latency = time.monotonic() - start_time
        return True, latency

    except Exception as e:
        latency = time.monotonic() - start_time
        print(f"Persistence health check failed: {e}")
        return False, latency

    finally:
        if connection is not None:
            connection.close()
