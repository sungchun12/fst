import os
import time
from watchdog.observers import Observer
from fst.query_handler import QueryHandler
import logging

logger = logging.getLogger(__name__)

observer = None

def watch_directory(directory: str, callback, active_file_path: str):
    global observer
    logger.info("Started watching directory...")
    event_handler = QueryHandler(callback, active_file_path)
    observer = Observer()
    observer.schedule(event_handler, path=directory, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
        logger.info("Stopped watching directory.")