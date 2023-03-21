from watchdog.events import FileSystemEventHandler
from threading import Timer
import logging

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
