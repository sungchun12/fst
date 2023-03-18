# duckdb_query.py
import duckdb
import os
import time
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import psutil
import subprocess


CURRENT_WORKING_DIR = os.getcwd()


class QueryHandler(FileSystemEventHandler):
    def __init__(self, callback):
        self.callback = callback

    def on_modified(self, event):
        if event.src_path.endswith(".sql"):
            active_file = get_active_file()
            if active_file and active_file == event.src_path:
                with open(event.src_path, "r") as file:
                    query = file.read()
                    self.callback(query)


def execute_query(query: str):
    connection = duckdb.connect()
    result = connection.execute(query).fetchall()
    connection.close()
    return result


def watch_directory(directory: str, callback):
    event_handler = QueryHandler(callback)
    observer = Observer()
    observer.schedule(event_handler, path=directory, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


def get_active_file():
    active_file = None
    for proc in psutil.process_iter(["name", "open_files"]):
        if proc.info["name"] == "Code":
            for file in proc.info["open_files"]:
                if file.path.endswith(".sql"):
                    active_file = file.path
                    break
    return active_file


if __name__ == "__main__":

    def handle_query(query):
        if query.strip():
            try:
                active_file = get_active_file()
                if not active_file:
                    return
                model_name = get_model_name_from_file(active_file)
                print(f"Compiling dbt with the modified SQL file ({model_name})...")
                result = subprocess.run(
                    ["dbt", "compile", "--models", model_name],
                    capture_output=True,
                    text=True,
                )
                if result.returncode == 0:
                    print("dbt compile was successful.")
                    compiled_sql_file = find_compiled_sql_file()
                    if compiled_sql_file:
                        with open(compiled_sql_file, "r") as file:
                            compiled_query = file.read()
                            print(f"Executing compiled query:\n{compiled_query}")
                            result = execute_query(compiled_query)
                            print("Result:")
                            for row in result:
                                print(row)
                    else:
                        print("Couldn't find the compiled SQL file.")
                else:
                    print("Error running dbt compile:")
                    print(result.stderr)
            except Exception as e:
                print(f"Error: {e}")
        else:
            print("Empty query.")

    project_directory = CURRENT_WORKING_DIR
    watch_directory(project_directory, handle_query)


def find_compiled_sql_file():
    active_file = get_active_file()
    if not active_file:
        return None
    project_directory = CURRENT_WORKING_DIR
    relative_file_path = os.path.relpath(active_file, project_directory)
    compiled_directory = os.path.join(project_directory, "target", "compiled")
    compiled_file_path = os.path.join(compiled_directory, relative_file_path)
    return compiled_file_path if os.path.exists(compiled_file_path) else None


def get_model_name_from_file(file_path: str):
    project_directory = CURRENT_WORKING_DIR
    models_directory = os.path.join(project_directory, "models")
    relative_file_path = os.path.relpath(file_path, models_directory)
    model_name, _ = os.path.splitext(relative_file_path)
    return model_name.replace(os.sep, ".")
