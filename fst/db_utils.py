import duckdb
from functools import lru_cache
from fst.config_defaults import PROFILES
import logging
from typing import List, Tuple, Any

logger = logging.getLogger(__name__)

@lru_cache(maxsize=128)
def execute_query(query: str, db_file: str) -> Tuple[List[Tuple[Any]], List[str]]:
    connection = duckdb.connect(database=db_file, read_only=False)
    result = connection.execute(query).fetchmany(5)
    column_names = [desc[0] for desc in connection.description]
    connection.close()
    return result, column_names

@lru_cache(maxsize=1)
def get_duckdb_file_path() -> str:
    target = PROFILES["jaffle_shop"]["target"]
    db_path = PROFILES["jaffle_shop"]["outputs"][target]["path"]
    return db_path

@lru_cache(maxsize=1)
def get_project_name() -> str:
    project_name = list(PROFILES.keys())[0]
    logger.info(f"project_name: {project_name}")
    return project_name
