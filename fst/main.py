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
@click.option(
    "--file-path",
    default=None,
    help="Path to the SQL file to be watched.",
)
def start(file_path):
    project_dir = os.path.abspath(".")
    models_dir = get_models_directory(project_dir)

    if file_path is None:
        click.echo(f"Started watching directory dynamically: {models_dir}")
        event_handler = DynamicQueryHandler(handle_query, models_dir)
    else:
        click.echo(f"Started watching file: {file_path}")
        event_handler = QueryHandler(handle_query, file_path)

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
