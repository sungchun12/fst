import click
import os
import subprocess
import multiprocessing

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


def start_directory_watcher(path):
    project_dir = path
    models_dir = get_models_directory(project_dir)

    event_handler = DynamicQueryHandler(handle_query, models_dir)
    watch_directory(event_handler, models_dir)


@main.command()
@click.option(
    "--path",
    "-p",
    default=CURRENT_WORKING_DIR,
    type=click.Path(exists=True, dir_okay=True, readable=True, resolve_path=True),
    help="dbt project root directory. Defaults to current working directory.",
)
def start(path):
    # Start the directory watcher and Streamlit app in separate processes
    streamlit_process = multiprocessing.Process(target=start_streamlit)
    dir_watcher_process = multiprocessing.Process(
        target=start_directory_watcher, args=(path,)
    )
    dir_watcher_process.start()
    streamlit_process.start()

    dir_watcher_process.join()
    streamlit_process.join()


if __name__ == "__main__":
    main()
