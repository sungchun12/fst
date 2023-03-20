import duckdb
import os
import re
import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess
import yaml
import sys
from tabulate import tabulate
from pygments import highlight
from pygments.lexers import SqlLexer
from pygments.formatters import TerminalFormatter
from functools import lru_cache
from threading import Timer
from termcolor import colored
import logging
from colorlog import ColoredFormatter

observer = None


def setup_logger():
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

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)


CURRENT_WORKING_DIR = os.getcwd()

# Load profiles.yml only once
with open("profiles.yml", "r") as file:
    PROFILES = yaml.safe_load(file)


class QueryHandler(FileSystemEventHandler):
    def __init__(self, callback, active_file_path: str):
        self.callback = callback
        self.active_file_path = active_file_path
        self.debounce_timer = None

    def on_modified(self, event):
        if event.src_path.endswith(".sql"):
            active_file = get_active_file(self.active_file_path)
            if active_file and active_file == event.src_path:
                if self.debounce_timer is None:
                    self.debounce_timer = Timer(1.5, self.debounce_query)
                    self.debounce_timer.start()
                else:
                    self.debounce_timer.cancel()
                    self.debounce_timer = Timer(1.5, self.debounce_query)
                    self.debounce_timer.start()

    def debounce_query(self):
        if self.debounce_timer is not None:
            self.debounce_timer.cancel()
            self.debounce_timer = None
        query = None
        with open(self.active_file_path, "r") as file:
            query = file.read()
        if query is not None and query.strip():
            logging.info(f"Detected modification: {self.active_file_path}")
            self.callback(query, self.active_file_path)


@lru_cache
def execute_query(query: str, db_file: str):
    connection = duckdb.connect(database=db_file, read_only=False)
    result = connection.execute(query).fetchmany(5)
    column_names = [desc[0] for desc in connection.description]
    connection.close()
    return result, column_names


def watch_directory(directory: str, callback, active_file_path: str):
    global observer
    setup_logger()
    logging.info("Started watching directory...")
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
        logging.info("Stopped watching directory.")


def get_active_file(file_path: str):
    if file_path and file_path.endswith(".sql"):
        return file_path
    else:
        logging.warning("No active SQL file found.")
        return None


@lru_cache
def get_project_name():
    project_name = list(PROFILES.keys())[0]
    logging.info(f"project_name: {project_name}")
    return project_name


def find_compiled_sql_file(file_path):
    active_file = get_active_file(file_path)
    if not active_file:
        return None
    project_directory = CURRENT_WORKING_DIR
    project_name = get_project_name()
    relative_file_path = os.path.relpath(active_file, project_directory)
    compiled_directory = os.path.join(
        project_directory, "target", "compiled", project_name
    )
    compiled_file_path = os.path.join(compiled_directory, relative_file_path)
    return compiled_file_path if os.path.exists(compiled_file_path) else None


def get_model_name_from_file(file_path: str):
    project_directory = CURRENT_WORKING_DIR
    models_directory = os.path.join(project_directory, "models")
    relative_file_path = os.path.relpath(file_path, models_directory)
    model_name, _ = os.path.splitext(relative_file_path)
    return model_name.replace(os.sep, ".")


@lru_cache
def get_duckdb_file_path():
    target = PROFILES["jaffle_shop"]["target"]
    db_path = PROFILES["jaffle_shop"]["outputs"][target]["path"]
    return db_path


def generate_test_yaml(model_name, column_names, active_file_path):
    test_yaml = f"version: 2\n\nmodels:\n  - name: {model_name}\n    columns:"

    for column in column_names:
        test_yaml += f"\n      - name: {column}\n        description: 'A placeholder description for {column}'"

        if re.search(r"(_id|_ID)$", column):
            test_yaml += "\n        tests:\n          - unique\n          - not_null"

    active_file_directory = os.path.dirname(active_file_path)
    active_file_name, _ = os.path.splitext(os.path.basename(active_file_path))
    new_yaml_file_name = f"{active_file_name}.yml"
    new_yaml_file_path = os.path.join(active_file_directory, new_yaml_file_name)

    with open(new_yaml_file_path, "w") as file:
        file.write(test_yaml)

    return new_yaml_file_path


def handle_query(query, file_path):
    if query.strip():
        try:
            start_time = time.time()

            active_file = get_active_file(file_path)
            if not active_file:
                return
            model_name = get_model_name_from_file(active_file)
            logging.info(
                f"Running `dbt build` with the modified SQL file ({model_name})..."
            )
            result = subprocess.run(
                ["dbt", "build", "--select", model_name],
                capture_output=True,
                text=True,
            )
            compile_time = time.time() - start_time

            stdout_without_finished = result.stdout.split("Finished running")[0]

            if result.returncode == 0:
                logging.info("`dbt build` was successful.")
                logging.info(result.stdout)
            else:
                logging.error("Error running `dbt build`:")
                logging.error(result.stdout)

            if (
                "PASS" not in stdout_without_finished
                and "FAIL" not in stdout_without_finished
                and "ERROR" not in stdout_without_finished
            ):
                compiled_sql_file = find_compiled_sql_file(file_path)
                if compiled_sql_file:
                    with open(compiled_sql_file, "r") as file:
                        compiled_query = file.read()
                    duckdb_file_path = get_duckdb_file_path()
                    _, column_names = execute_query(compiled_query, duckdb_file_path)

                    warning_message = colored(
                        "Warning: No tests were run with the `dbt build` command. Consider adding tests to your project.",
                        "yellow",
                        attrs=["bold"],
                    )
                    logging.warning(warning_message)

                    test_yaml_path = generate_test_yaml(
                        model_name, column_names, active_file
                    )
                    test_yaml_path_warning_message = colored(
                        f"Generated test YAML file: {test_yaml_path}",
                        "yellow",
                        attrs=["bold"],
                    )
                    logging.warning(test_yaml_path_warning_message)

            compiled_sql_file = find_compiled_sql_file(file_path)
            if compiled_sql_file:
                with open(compiled_sql_file, "r") as file:
                    compiled_query = file.read()
                    colored_compiled_query = highlight(
                        compiled_query, SqlLexer(), TerminalFormatter()
                    )
                    logging.info(f"Executing compiled query from: {compiled_sql_file}")
                    duckdb_file_path = get_duckdb_file_path()
                    logging.info(f"Using DuckDB file: {duckdb_file_path}")

                    start_time = time.time()
                    result, column_names = execute_query(
                        compiled_query, duckdb_file_path
                    )
                    query_time = time.time() - start_time

                    logging.info(f"`dbt build` time: {compile_time:.2f} seconds")
                    logging.info(f"Query time: {query_time:.2f} seconds")

                    logging.info(
                        "Result Preview"
                        + "\n"
                        + tabulate(result, headers=column_names, tablefmt="grid")
                    )
            else:
                logging.error("Couldn't find the compiled SQL file.")
        except Exception as e:
            logging.error(f"Error: {e}")
    else:
        logging.error("Empty query.")


if __name__ == "__main__":
    setup_logger()
    if len(sys.argv) > 1:
        active_file_path = sys.argv[1]
    else:
        active_file_path = None

    project_directory = CURRENT_WORKING_DIR
    logging.info(f"Watching directory: {project_directory}")
    watch_directory(project_directory, handle_query, active_file_path)

awoifjoawijf