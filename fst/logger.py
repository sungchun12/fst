import logging
import sys
from colorlog import ColoredFormatter
from multiprocessing import Queue, current_process
from queue import Empty

class QueueHandler(logging.Handler):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def emit(self, record):
        self.queue.put(record)

def setup_logger(queue=None):
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
    logger.setLevel(logging.INFO)

    if queue is not None:
        handler = QueueHandler(queue)
    else:
        handler = logging.StreamHandler(sys.stdout)

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger
