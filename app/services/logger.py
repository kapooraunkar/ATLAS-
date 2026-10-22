import logging
import sys
from contextvars import ContextVar

request_id_context: ContextVar[str] = ContextVar(
    "request_id",
    default="-",
)

class RequestContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        request_id = request_id_context.get()
        if request_id and request_id != "-":
            record.request_id = f"request_id={request_id}"
        else:
            record.request_id = ""

        return True 

class AtlasLogger:
    def __init__(self,name: str):
        self.logger = logging.getLogger(name)

        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)

            
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s "
                "%(request_id)s | %(message)s"
            )

            handler.setFormatter(formatter)
            handler.addFilter(RequestContextFilter())
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def info(self, message: str):
        self.logger.info(message)

    def warning(self,message: str):
        self.logger.warning(message)

    def error(self,message: str):
        self.logger.error(message)

logger = AtlasLogger("atlas")