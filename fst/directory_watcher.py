import os
import time
from watchdog.observers.polling import PollingObserver
import logging

logger = logging.getLogger(__name__)

observer = None


def watch_directory(
    event_handler,
    file_path: str,
):
    global observer
    logger.info(f"Started watching directory: {file_path}")
    observer = PollingObserver()
    observer.schedule(event_handler, path=file_path, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
        logger.info(f"Stopped watching directory: {file_path}")