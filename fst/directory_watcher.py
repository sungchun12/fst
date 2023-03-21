import os
from watchdog.observers import Observer
from fst.query_handler import QueryHandler

def watch_directory(directory: str, callback, active_file_path: str):
    event_handler = QueryHandler(callback, active_file_path)
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
