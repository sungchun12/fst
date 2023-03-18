import duckdb
import os
import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess
import yaml
import sys
from tabulate import tabulate


CURRENT_WORKING_DIR = os.getcwd()


class QueryHandler(FileSystemEventHandler):
    def __init__(self, callback, active_file_path: str):
        self.callback = callback
        self.active_file_path = active_file_path

    def on_modified(self, event):
        if event.src_path.endswith(".sql"):
            active_file = get_active_file(self.active_file_path)
            if active_file and active_file == event.src_path:
                print(f"Detected modification: {event.src_path}")
                with open(event.src_path, "r") as file:
                    query = file.read()
                    self.callback(query, self.active_file_path)


def execute_query(query: str, db_file: str):
    connection = duckdb.connect(database=db_file, read_only=False)
    result = connection.execute(query).fetchmany(10)
    connection.close()
    return result


def watch_directory(directory: str, callback, active_file_path: str):
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


def get_active_file(file_path: str):
    if file_path and file_path.endswith(".sql"):
        return file_path
    else:
        print("No active SQL file found.")
        return None


def get_project_name():
    with open("profiles.yml", "r") as file:
        profiles = yaml.safe_load(file)
        project_name = list(profiles.keys())[0]
        print(f"project_name: {project_name}")
        return project_name


def find_compiled_sql_file(file_path):
    active_file = get_active_file(file_path)
    if not active_file:
        return None
    project_directory = CURRENT_WORKING_DIR
    project_name = get_project_name()
    relative_file_path = os.path.relpath(active_file, project_directory)
    compiled_directory = os.path.join(project_directory, "target", "compiled", project_name)
    compiled_file_path = os.path.join(compiled_directory, relative_file_path)
    print(f"compiled_file_path: {compiled_file_path}")
    return compiled_file_path if os.path.exists(compiled_file_path) else None


def get_model_name_from_file(file_path: str):
    project_directory = CURRENT_WORKING_DIR
    models_directory = os.path.join(project_directory, "models")
    relative_file_path = os.path.relpath(file_path, models_directory)
    model_name, _ = os.path.splitext(relative_file_path)
    return model_name.replace(os.sep, ".")


def get_duckdb_file_path():
    with open("profiles.yml", "r") as file:
        profiles = yaml.safe_load(file)
        target = profiles["jaffle_shop"]["target"]
        db_path = profiles["jaffle_shop"]["outputs"][target]["path"]
        return db_path



def handle_query(query, file_path):
    print(f"Received query:\n{query}")
    if query.strip():
        try:
            start_time = time.time()  # Add this line

            active_file = get_active_file(file_path)
            if not active_file:
                return
            model_name = get_model_name_from_file(active_file)
            print(f"Compiling dbt with the modified SQL file ({model_name})...")
            result = subprocess.run(
                ["dbt", "compile", "--models", model_name],
                capture_output=True,
                text=True,
            )
            compile_time = time.time() - start_time  # Add this line

            if result.returncode == 0:
                print("dbt compile was successful.")
                compiled_sql_file = find_compiled_sql_file(file_path)
                if compiled_sql_file:
                    with open(compiled_sql_file, "r") as file:
                        compiled_query = file.read()
                        print(f"Executing compiled query:\n{compiled_query}")
                        duckdb_file_path = get_duckdb_file_path()
                        print(f"Using DuckDB file: {duckdb_file_path}")

                        start_time = time.time()  # Add this line
                        result = execute_query(compiled_query, duckdb_file_path)
                        query_time = time.time() - start_time  # Add this line

                        print(f"Compilation time: {compile_time:.2f} seconds")
                        print(f"Query time: {query_time:.2f} seconds")

                        print("Result:")
                        headers = ["ID", "First Name", "Last Initial", "Created At", "Updated At", "Orders Count", "Total Spent"]
                        print(tabulate(result, headers=headers, tablefmt="grid"))
                else:
                    print("Couldn't find the compiled SQL file.")
            else:
                print("Error running dbt compile:")
                print(result.stderr)
        except Exception as e:
            print(f"Error: {e}")
    else:
        print("Empty query.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        active_file_path = sys.argv[1]
    else:
        active_file_path = None

    project_directory = CURRENT_WORKING_DIR
    print(f"Watching directory: {project_directory}")
    watch_directory(project_directory, handle_query, active_file_path)
