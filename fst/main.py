import os
import time
import click
from watchdog.observers.polling import PollingObserver

from fst.file_utils import get_models_directory
from fst.query_handler import handle_query, DynamicQueryHandler
from fst.directory_watcher import watch_directory
from fst.logger import setup_logger


@click.group()
def main():
    setup_logger()
    pass


@main.command()
def start():
    project_dir = os.path.abspath(".")
    models_dir = get_models_directory(project_dir)

    event_handler = DynamicQueryHandler(handle_query, models_dir)
    watch_directory(event_handler, models_dir)


if __name__ == "__main__":
    main()
