import click
import os
import yaml
from fst import fst_query


@click.group()
def main():
    pass


def get_model_paths():
    with open("dbt_project.yml", "r") as file:
        dbt_project = yaml.safe_load(file)
        model_paths = dbt_project.get("model-paths", [])
        return [
            os.path.join(fst_query.CURRENT_WORKING_DIR, path) for path in model_paths
        ]


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
        fst_query.watch_directory(path, fst_query.handle_query, fst_query.find_compiled_sql_file)
    elif model_paths:
        for path in model_paths:
            click.echo(f"Started watching directory: {path}")
            fst_query.watch_directory(path, fst_query.handle_query, fst_query.find_compiled_sql_file)
    else:
        click.echo("Please provide a file path using the --file-path option.")


@main.command()
def stop():
    if fst_query.observer:
        fst_query.observer.stop()
        fst_query.observer.join()
        click.echo("Stopped watching the directory.")
    else:
        click.echo("No observer is currently running.")


@main.command()
@click.option(
    "--file-path",
    type=click.Path(
        exists=True, file_okay=True, dir_okay=False, readable=True, resolve_path=True
    ),
    help="Path to the SQL file you want to watch.",
)
def restart(file_path):
    if fst_query.observer:
        fst_query.observer.stop()
        fst_query.observer.join()
        click.echo("Stopped watching the directory.")
    else:
        click.echo("No observer was running. Starting a new one.")

    model_paths = get_model_paths()
    if file_path:
        click.echo(f"Started watching directory: {os.path.dirname(file_path)}")
        fst_query.watch_directory(path, fst_query.handle_query, fst_query.find_compiled_sql_file)
    elif model_paths:
        for path in model_paths:
            click.echo(f"Started watching directory: {path}")
            fst_query.watch_directory(path, fst_query.handle_query, fst_query.find_compiled_sql_file)
    else:
        click.echo("Please provide a file path using the --file-path option.")


if __name__ == "__main__":
    main()
