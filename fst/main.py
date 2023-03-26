import click
import os
import subprocess
import multiprocessing
import logging

from fst.file_utils import get_models_directory
from fst.query_handler import handle_query, DynamicQueryHandler
from fst.directory_watcher import watch_directory
from fst.logger import setup_logger
from fst.config_defaults import CURRENT_WORKING_DIR


@click.group()
def main():
    setup_logger()
    pass


def start_streamlit():
    # Get the path to the Streamlit app script relative to this file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    streamlit_app_path = os.path.join(current_dir, "streamlit_copilot.py")

    # Run the Streamlit app
    subprocess.run(["streamlit", "run", streamlit_app_path])


def start_directory_watcher(path, log_queue):
    setup_logger(log_queue)
    project_dir = path
    models_dir = get_models_directory(project_dir)

    event_handler = DynamicQueryHandler(handle_query, models_dir)
    watch_directory(event_handler, models_dir)


def listener_process(queue):
    setup_logger()
    while True:
        try:
            record = queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Empty:
            pass


@main.command()
@click.option(
    "--path",
    "-p",
    default=CURRENT_WORKING_DIR,
    type=click.Path(exists=True, dir_okay=True, readable=True, resolve_path=True),
    help="dbt project root directory. Defaults to current working directory.",
)
def start(path):
    log_queue = multiprocessing.Queue()

    # Start the directory watcher and Streamlit app in separate processes
    dir_watcher_process = multiprocessing.Process(
        target=start_directory_watcher, args=(path, log_queue)
    )
    streamlit_process = multiprocessing.Process(target=start_streamlit)

    listener = multiprocessing.Process(target=listener_process, args=(log_queue,))

    listener.start()
    dir_watcher_process.start()
    streamlit_process.start()

    dir_watcher_process.join()
    streamlit_process.join()

    log_queue.put(None)
    listener.join()


if __name__ == "__main__":
    main()
