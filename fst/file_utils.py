import os
import yaml
import logging
import re
from fst.config_defaults import CURRENT_WORKING_DIR
from fst.db_utils import get_project_name

logger = logging.getLogger(__name__)

def get_active_file(file_path: str):
    if file_path and file_path.endswith(".sql"):
        return file_path
    else:
        logger.warning("No active SQL file found.")
        return None

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

def find_tests_for_model(model_name, directory='models'):
    """
    Check if tests are generated for a given model in a dbt project.

    Args:
        model_name (str): The name of the model to search for tests.
        directory (str, optional): The root directory to start the search. Defaults to 'models'.

    Returns:
        dict: A dictionary containing information about the tests found, including the model name, column name, file type, and tests.
    """
    tests_data = {}

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(('.schema.yml', '.yml')):
                filepath = os.path.join(root, file)
                with open(filepath, 'r') as f:
                    schema_data = yaml.safe_load(f)

                for model in schema_data.get('models', []):
                    if model['name'] == model_name:
                        columns = model.get('columns', {})
                        for column_data in columns:
                            column_name = column_data['name']
                            tests = column_data.get('tests', [])
                            if tests:
                                tests_data.append({'file': filepath, 'column': column_name, 'tests': tests})

    return tests_data



import yaml
import re
import os

def generate_test_yaml(model_name, column_names, active_file_path, tests_data):
    yaml_files = {}

    for column in column_names:
        tests_to_add = []
        if re.search(r"(_id|_ID)$", column):
            tests_to_add = ["unique", "not_null"]

        # Check if tests for this column already exist
        existing_tests = [data for data in tests_data if data['column'] == column]

        if existing_tests:
            # Update the existing YAML file with new tests
            for test_data in existing_tests:
                yaml_file = test_data['file']
                if yaml_file not in yaml_files:
                    with open(yaml_file, 'r') as f:
                        yaml_files[yaml_file] = yaml.safe_load(f)

                models = yaml_files[yaml_file].get('models', [])
                for model in models:
                    if model['name'] == model_name:
                        columns = model.get('columns', [])
                        for existing_column in columns:
                            if existing_column['name'] == column:
                                tests = existing_column.get('tests', [])
                                for test in tests_to_add:
                                    if test not in tests:
                                        tests.append(test)
                                existing_column['tests'] = tests
        else:
            # If no tests exist, add the tests to the schema.yml file
            schema_yml_path = os.path.join(os.path.dirname(active_file_path), "schema.yml")
            if os.path.exists(schema_yml_path):
                with open(schema_yml_path, "r") as f:
                    schema_yml_data = yaml.safe_load(f)

                for model in schema_yml_data.get("models", []):
                    if model["name"] == model_name:
                        if "columns" not in model:
                            model["columns"] = []

                        new_column = {
                            "name": column,
                            "description": f"A placeholder description for {column}",
                            "tests": tests_to_add,
                        }
                        model["columns"].append(new_column)
                        break

                with open(schema_yml_path, "w") as f:
                    yaml.dump(schema_yml_data, f)

                return schema_yml_path

    # Return the first file path where tests were found
    return next(iter(yaml_files))


def get_model_paths():
    with open("dbt_project.yml", "r") as file:
        dbt_project = yaml.safe_load(file)
        model_paths = dbt_project.get("model-paths", [])
        return [
            os.path.join(os.getcwd(), path) for path in model_paths
        ]

def get_models_directory(project_dir):
    dbt_project_file = os.path.join(project_dir, 'dbt_project.yml')
    with open(dbt_project_file, 'r') as file:
        dbt_project = yaml.safe_load(file)
    models_subdir = dbt_project.get('model-paths')[0]
    return os.path.join(project_dir, models_subdir)