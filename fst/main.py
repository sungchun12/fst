import click

from fst.file_utils import get_models_directory
from fst.query_handler import handle_query, DynamicQueryHandler
from fst.directory_watcher import watch_directory
from fst.logger import setup_logger
from fst.config_defaults import CURRENT_WORKING_DIR


@click.group()
def main():
    setup_logger()
    pass


@main.command()
@click.option(
    "--path",
    "-p",
    default=CURRENT_WORKING_DIR,
    type=click.Path(
        exists=True, dir_okay=True, readable=True, resolve_path=True
    ),
    help="dbt project root directory. Defaults to current working directory.",
)
def start(path):
    project_dir = path
    models_dir = get_models_directory(project_dir)

    event_handler = DynamicQueryHandler(handle_query, models_dir)
    watch_directory(event_handler, models_dir)


if __name__ == "__main__":
    main()
