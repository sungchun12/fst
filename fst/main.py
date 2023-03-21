import click
import os
from fst.logger import setup_logger
from fst.query_handler import handle_query
from fst.directory_watcher import watch_directory
from fst.file_utils import get_active_file, get_model_paths

@click.group()
def main():
    pass

@main.command()
@click.option(
    "--file-path",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True
    ),
    help="Path to the SQL file you want to watch.",
)
def start(file_path):
    model_paths = get_model_paths()
    if file_path:
        click.echo(f"Started watching directory: {os.path.dirname(file_path)}")
        watch_directory(
            os.path.dirname(file_path), handle_query, file_path
        )
    elif model_paths:
        for path in model_paths:
            click.echo(f"Started watching directory: {path}")
            watch_directory(path, handle_query, None)
    else:
        click.echo("Please provide a file path using the --file-path option.")

if __name__ == "__main__":
    main()
