from watchdog.events import FileSystemEventHandler
from threading import Timer
import logging
import os
import time
import subprocess
from tabulate import tabulate

from fst.file_utils import (
    get_active_file,
    find_tests_for_model,
    get_model_name_from_file,
    find_compiled_sql_file,
    generate_test_yaml,
)
from fst.db_utils import get_duckdb_file_path, execute_query
from fst.config_defaults import DISABLE_TESTS

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


    
def generate_and_run_tests(model_name, column_names, active_file):
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
            ["dbt", "test", "--select", model_name],
            capture_output=True,
            text=True,
        )
        if result_rerun.returncode == 0:
            logger.info("`dbt test` with generated tests was successful.")
        else:
            logger.info("`dbt test` with generated tests failed.")
            
        logger.info(result_rerun.stdout)
    else:
        logger.error("Couldn't find the generated test YAML file.")

def handle_query(query, file_path):
    if not query.strip():
        logger.info("Query is empty.")
        return
    
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

        if result.returncode == 0:
            logger.info("`dbt build` was successful.")
            logger.info(result.stdout)
        else:
            logger.error("Error running `dbt build`:")
            logger.error(result.stdout)

        
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
                 # Check if tests are generated for the model
                tests_exist = find_tests_for_model(model_name) 

                if not tests_exist and not DISABLE_TESTS:
                    response = input(f"No tests found for the '{model_name}' model. Would you like to generate tests? (yes/no): ")

                    if response.lower() == 'yes':
                        logger.info(f"Generating tests for the '{model_name}' model...")
                        generate_and_run_tests(model_name, column_names, active_file)
                    else:
                        logger.info(f"Skipping tests generation for the '{model_name}' model.")   

        else:
            logger.error("Couldn't find the compiled SQL file.")
    except Exception as e:
        logger.error(f"Error: {e}")
