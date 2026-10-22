import time
from queue import Full
import uuid

import psutil
from fastapi import Request

from app.schemas.telemetry import Telemetry
from app.services.telemetry_queue import (
    add_to_queue,
    record_dropped_telemetry,
    record_ingested,
)


async def telemetry(request: Request, call_next):

    start_time = time.perf_counter()

    request_id = str(uuid.uuid4())

    try:
        response = await call_next(request)

        duration_seconds = time.perf_counter() - start_time

        telemetry = Telemetry(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_seconds * 1000,
            cpu_percent=psutil.cpu_percent(),
            memory_percent=psutil.virtual_memory().percent,
        )

        try:
            accepted = add_to_queue(telemetry)

            if accepted:
                record_ingested()
            else:
                record_dropped_telemetry()

                print(
                    f"Telemetry queue full. "
                    f"Dropping telemetry for request {telemetry.request_id}"
                )
        except Exception as e:
            print(f"Failed to queue telemetry: {e}")

        return response

    except Exception as e:

        duration_seconds = time.perf_counter() - start_time

        telemetry = Telemetry(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            status_code=500,
            duration_ms=duration_seconds * 1000,
            cpu_percent=psutil.cpu_percent(),
            memory_percent=psutil.virtual_memory().percent,
        )

        try:
            accepted = add_to_queue(telemetry)
        
            if accepted:
                record_ingested()
            else:
                record_dropped_telemetry()
        
                print(
                    f"Telemetry queue full. "
                    f"Dropping telemetry for request {telemetry.request_id}"
                )
        except Exception as e:
            print(f"Failed to queue telemetry: {e}")
        

        print(f"Exception Type : {type(e).__name__}")
        print(f"Message        : {e}")

        raise