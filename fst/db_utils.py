import duckdb
import os
from functools import lru_cache
from fst.config_defaults import PROFILES
import logging

logger = logging.getLogger(__name__)

@lru_cache
def execute_query(query: str, db_file: str):
    connection = duckdb.connect(database=db_file, read_only=False)
    result = connection.execute(query).fetchmany(5)
    column_names = [desc[0] for desc in connection.description]
    connection.close()
    return result, column_names

@lru_cache
def get_duckdb_file_path():
    target = PROFILES["jaffle_shop"]["target"]
    db_path = PROFILES["jaffle_shop"]["outputs"][target]["path"]
    return db_path


@lru_cache
def get_project_name():
    project_name = list(PROFILES.keys())[0]
    logger.info(f"project_name: {project_name}")
    return project_name
