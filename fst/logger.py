import logging
import sys
from colorlog import ColoredFormatter
from multiprocessing import Queue
from typing import Optional

class QueueHandler(logging.Handler):
    def __init__(self, queue: Queue):
        super().__init__()
        self.queue = queue

    def emit(self, record: logging.LogRecord) -> None:
        self.queue.put(record)

def setup_logger(queue: Optional[Queue] = None) -> logging.Logger:
    log_format = "%(asctime)s - %(levelname)s - %(log_color)s%(message)s%(reset)s"

    formatter = ColoredFormatter(
        log_format,
        datefmt="%Y-%m-%d %H:%M:%S",
        reset=True,
        log_colors={
            "DEBUG": "cyan",
            "INFO": "light_blue",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
    )

    logger = logging.getLogger()
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        if queue is not None:
            handler = QueueHandler(queue)
        else:
            handler = logging.StreamHandler(sys.stdout)

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger