import click
import os
import subprocess
import multiprocessing
import logging

from fst.file_utils import get_models_directory
from fst.query_handler import handle_query, DynamicQueryHandler
from fst.directory_watcher import watch_directory
from fst.logger import setup_logger
from fst.config_defaults import CURRENT_WORKING_DIR, CONFIG


logger = logging.getLogger(__name__)

@click.group()
def main() -> None:
    setup_logger()
    pass

def start_streamlit() -> None:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    streamlit_app_path = os.path.join(current_dir, "streamlit_copilot.py")
    subprocess.run(["streamlit", "run", streamlit_app_path])

def start_directory_watcher(path: str, log_queue: multiprocessing.Queue) -> None:
    project_dir = path
    models_dir = get_models_directory(project_dir)
    event_handler = DynamicQueryHandler(handle_query, models_dir)
    watch_directory(event_handler, models_dir)

def listener_process(queue: multiprocessing.Queue) -> None:
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
@click.option(
    "--preview-limit",
    default=CONFIG["PREVIEW_LIMIT_ROWS"],
    type=int,
    help="Set the number of rows to preview in the terminal and copilot UI. Defaults to 5.",
)
def start(path: str, preview_limit: int) -> None:
    CONFIG["PREVIEW_LIMIT_ROWS"] = preview_limit
    log_queue = multiprocessing.Queue()
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
