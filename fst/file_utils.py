import os
import yaml
import logging
import re
from typing import List, Optional
from fst.config_defaults import CURRENT_WORKING_DIR
from fst.db_utils import get_project_name

logger = logging.getLogger(__name__)

def get_active_file(file_path: str) -> Optional[str]:
    if file_path and file_path.endswith(".sql"):
        return file_path
    else:
        logger.warning("No active SQL file found.")
        return None

def find_compiled_sql_file(file_path: str) -> Optional[str]:
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

def get_model_name_from_file(file_path: str) -> str:
    project_directory = CURRENT_WORKING_DIR
    models_directory = os.path.join(project_directory, "models")
    relative_file_path = os.path.relpath(file_path, models_directory)
    model_name, _ = os.path.splitext(relative_file_path)
    return model_name.replace(os.sep, ".")

def generate_test_yaml(model_name: str, column_names: List[str], active_file_path: str) -> str:
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

def get_model_paths() -> List[str]:
    with open("dbt_project.yml", "r") as file:
        dbt_project = yaml.safe_load(file)
        model_paths = dbt_project.get("model-paths", [])
        return [
            os.path.join(os.getcwd(), path) for path in model_paths
        ]

def get_models_directory(project_dir: str) -> str:
    dbt_project_file = os.path.join(project_dir, 'dbt_project.yml')
    with open(dbt_project_file, 'r') as file:
        dbt_project = yaml.safe_load(file)
    models_subdir = dbt_project.get('model-paths')[0]
    return os.path.join(project_dir, models_subdir)
