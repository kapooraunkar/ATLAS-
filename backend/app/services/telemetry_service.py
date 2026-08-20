from app.schemas.telemetry import Telemetry
from app.database.connection import get_connection
import time 

def process_telemetry(telemetry: Telemetry):

    raise Exception("TEST: SQLite persistence failure")
    
    print("\n========== TELEMETRY ==========")
    print(f"Request ID  : {telemetry.request_id}")
    print(f"Method      : {telemetry.method}")
    print(f"Path        : {telemetry.path}")
    print(f"Status Code : {telemetry.status_code}")
    print(f"Duration    : {telemetry.duration_ms:.2f} ms")
    print(f"CPU         : {telemetry.cpu_percent:.1f}%")
    print(f"Memory      : {telemetry.memory_percent:.1f}%")
    print("===============================\n")

    connection = get_connection()

    connection.execute(
        """
        INSERT INTO telemetry (
            request_id,
            method,
            path,
            status_code,
            duration_ms,
            cpu_percent,
            memory_percent
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            telemetry.request_id,
            telemetry.method,
            telemetry.path,
            telemetry.status_code,
            telemetry.duration_ms,
            telemetry.cpu_percent,
            telemetry.memory_percent,
        ),
    )

    connection.commit()
    connection.close()