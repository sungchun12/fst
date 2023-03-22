from watchdog.events import FileSystemEventHandler
from threading import Timer
import logging
import os
import time
import subprocess
from tabulate import tabulate

from fst.file_utils import (
    get_active_file,
    get_model_name_from_file,
    find_compiled_sql_file,
    generate_test_yaml,
)
from fst.db_utils import get_duckdb_file_path, execute_query

logger = logging.getLogger(__name__)


class DynamicQueryHandler(FileSystemEventHandler):
    def __init__(self, callback, models_dir: str):
        self.callback = callback
        self.models_dir = models_dir
        self.debounce_timer = None

    def on_modified(self, event):
        if event.src_path.endswith(".sql"):
            if os.path.dirname(event.src_path) == self.models_dir:
                self.debounce()
                self.handle_query_for_file(event.src_path)

    def debounce(self):
        if self.debounce_timer is not None:
            self.debounce_timer.cancel()
        self.debounce_timer = Timer(1.5, self.debounce_query)
        self.debounce_timer.start()

    def debounce_query(self):
        if self.debounce_timer is not None:
            self.debounce_timer.cancel()
            self.debounce_timer = None

    def handle_query_for_file(self, file_path):
        with open(file_path, "r") as file:
            query = file.read()
        if query is not None and query.strip():
            handle_query(query, file_path)


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
            logger.info(f"Detected modification: {self.active_file_path}")
            self.callback(query, self.active_file_path)


def handle_query(query, file_path):
    if query.strip():
        try:
            start_time = time.time()

            active_file = get_active_file(file_path)
            if not active_file:
                return
            model_name = get_model_name_from_file(active_file)
            logger.info(
                f"Running `dbt build` with the modified SQL file ({active_file})..."
            )
            result = subprocess.run(
                ["dbt", "build", "--select", model_name],
                capture_output=True,
                text=True,
            )
            compile_time = time.time() - start_time

            stdout_without_finished = result.stdout.split("Finished running")[0]

            if result.returncode == 0:
                logger.info("`dbt build` was successful.")
                logger.info(result.stdout)
            else:
                logger.error("Error running `dbt build`:")
                logger.error(result.stdout)

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

                    warning_message = "Warning: No tests were run with the `dbt build` command. Consider adding tests to your project."

                    logger.warning(warning_message)

                    test_yaml_path = generate_test_yaml(
                        model_name, column_names, active_file
                    )
                    test_yaml_path_warning_message = (
                        f"Generated test YAML file: {test_yaml_path}"
                    )

                    logger.warning(test_yaml_path_warning_message)
                    logger.warning("Rerunning `dbt build` with the generated test YAML file...")
                    time.sleep(0.5)
                    result_rerun = subprocess.run(
                        ["dbt", "build", "--select", model_name],
                        capture_output=True,
                        text=True,
                    )
                    if result_rerun.returncode == 0:
                        logger.info("`dbt build` with generated tests was successful.")
                        logger.info(result.stdout)

            compiled_sql_file = find_compiled_sql_file(file_path)
            if compiled_sql_file:
                with open(compiled_sql_file, "r") as file:
                    compiled_query = file.read()
                    logger.info(f"Executing compiled query from: {compiled_sql_file}")
                    duckdb_file_path = get_duckdb_file_path()
                    logger.info(f"Using DuckDB file: {duckdb_file_path}")

                    start_time = time.time()
                    result, column_names = execute_query(
                        compiled_query, duckdb_file_path
                    )
                    query_time = time.time() - start_time

                    logger.info(f"`dbt build` time: {compile_time:.2f} seconds")
                    logger.info(f"Query time: {query_time:.2f} seconds")

                    logger.info(
                        "Result Preview"
                        + "\n"
                        + tabulate(result, headers=column_names, tablefmt="grid")
                    )
            else:
                logger.error("Couldn't find the compiled SQL file.")
        except Exception as e:
            logger.error(f"Error: {e}")
    else:
        logger.error("Empty query.")
