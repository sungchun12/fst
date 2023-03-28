from watchdog.events import FileSystemEventHandler
from threading import Timer
import logging
import os
import time
import subprocess
from tabulate import tabulate
import duckdb
import json
from datetime import date, datetime
from typing import Optional, Any

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
        self.debounce_timer: Optional[Timer] = None

    def on_modified(self, event) -> None:
        if event.src_path.endswith(".sql"):
            # Check if the modified file is in any subdirectory under models_dir
            if os.path.commonpath([self.models_dir, event.src_path]) == self.models_dir:
                self.debounce()
                self.handle_query_for_file(event.src_path)

    def debounce(self) -> None:
        if self.debounce_timer is not None:
            self.debounce_timer.cancel()
        self.debounce_timer = Timer(1.5, self.debounce_query)
        self.debounce_timer.start()

    def debounce_query(self) -> None:
        if self.debounce_timer is not None:
            self.debounce_timer.cancel()
            self.debounce_timer = None

    def handle_query_for_file(self, file_path: str) -> None:
        with open(file_path, "r") as file:
            query = file.read()
        if query is not None and query.strip():
            handle_query(query, file_path)


class DateEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, date):
            return obj.isoformat()
        return super(DateEncoder, self).default(obj)


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
                ["dbt", "build", "--select", model_name, "--store-failures"],
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

                    # Verify if the newly generated test YAML file exists
                    if os.path.isfile(test_yaml_path):
                        logger.warning(test_yaml_path_warning_message)
                        logger.warning(
                            "Running `dbt test` with the generated test YAML file..."
                        )
                        result_rerun = subprocess.run(
                            ["dbt", "test", "--select", model_name, "--store-failures"],
                            capture_output=True,
                            text=True,
                        )
                        if result_rerun.returncode == 0:
                            logger.info(
                                "`dbt test` with generated tests was successful."
                            )
                            logger.info(result_rerun.stdout)
                    else:
                        logger.error("Couldn't find the generated test YAML file.")

            compiled_sql_file = find_compiled_sql_file(file_path)
            if compiled_sql_file:
                with open(compiled_sql_file, "r") as file:
                    compiled_query = file.read()
                    logger.info(f"Executing compiled query from: {compiled_sql_file}")
                    duckdb_file_path = get_duckdb_file_path()
                    logger.info(f"Using DuckDB file: {duckdb_file_path}")

                    start_time = time.time()
                    preview_result, column_names = execute_query(
                        compiled_query, duckdb_file_path
                    )
                    query_time = time.time() - start_time

                    logger.info(f"`dbt build` time: {compile_time:.2f} seconds")
                    logger.info(f"Query time: {query_time:.2f} seconds")

                    logger.info(
                        "Result Preview"
                        + "\n"
                        + tabulate(
                            preview_result, headers=column_names, tablefmt="grid"
                        )
                    )
            else:
                logger.error("Couldn't find the compiled SQL file.")

            duckdb_conn = duckdb.connect("fst_metrics.duckdb")
            duckdb_conn.execute(
                """
                CREATE TABLE IF NOT EXISTS metrics (
                    timestamp TIMESTAMP,
                    modified_sql_file TEXT,
                    compiled_sql_file TEXT,
                    dbt_build_status TEXT,
                    duckdb_file_name TEXT,
                    dbt_build_time REAL,
                    query_time REAL,
                    result_preview_json TEXT
                )
            """
            )

            dbt_build_status = "success" if result.returncode == 0 else "failure"
            duckdb_file_path = get_duckdb_file_path()

            # Convert the result and column_names to JSON
            result_preview_dict = [
                dict(zip(column_names, row)) for row in preview_result
            ]
            # Use the custom DateEncoder to handle date objects
            result_preview_json = json.dumps(result_preview_dict, cls=DateEncoder)
            current_timestamp = datetime.datetime.now()

            duckdb_conn.execute(
                """
                INSERT INTO metrics (
                    timestamp,
                    modified_sql_file,
                    compiled_sql_file,
                    dbt_build_status,
                    duckdb_file_name,
                    dbt_build_time,
                    query_time,
                    result_preview_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    current_timestamp,
                    active_file,
                    compiled_sql_file,
                    dbt_build_status,
                    duckdb_file_path,
                    compile_time,
                    query_time,
                    result_preview_json,
                ),
            )

            duckdb_conn.commit()
            duckdb_conn.close()
            logger.info("fst metrics saved to the database: fst_metrics.duckdb")

        except Exception as e:
            logger.error(f"Error: {e}")
    else:
        logger.error("Empty query.")
