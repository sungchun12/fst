import os
import time
import click
from watchdog.observers.polling import PollingObserver

from fst.file_utils import get_models_directory
from fst.query_handler import handle_query, DynamicQueryHandler, QueryHandler
from fst.logger import setup_logger


@click.group()
def main():
    setup_logger()
    pass


@main.command()
def start():
    project_dir = os.path.abspath(".")
    models_dir = get_models_directory(project_dir)

    click.echo(f"Started watching directory dynamically: {models_dir}")
    event_handler = DynamicQueryHandler(handle_query, models_dir)

    observer = PollingObserver()
    observer.schedule(event_handler, models_dir, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()
