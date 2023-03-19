import click
from fst import fst_query


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
    if file_path:
        fst_query.watch_directory(
            fst_query.CURRENT_WORKING_DIR, fst_query.handle_query, file_path
        )
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

    if file_path:
        fst_query.watch_directory(
            fst_query.CURRENT_WORKING_DIR, fst_query.handle_query, file_path
        )
    else:
        click.echo("Please provide a file path using the --file-path option.")


if __name__ == "__main__":
    main()
