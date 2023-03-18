import argparse
import sys
import os
import signal
import subprocess
from pathlib import Path

PID_FILE = "fst.pid"


def start(args):
    if Path(PID_FILE).exists():
        print("FST is already running. Use 'fst stop' to stop the process.")
        sys.exit(1)

    process = subprocess.Popen(["python", "duckdb_query.py"])
    with open(PID_FILE, "w") as f:
        f.write(str(process.pid))
    print("FST started.")


def stop(args):
    if not Path(PID_FILE).exists():
        print("FST is not running. Use 'fst start' to start the process.")
        sys.exit(1)

    with open(PID_FILE, "r") as f:
        pid = int(f.read())

    try:
        os.kill(pid, signal.SIGTERM)
        Path(PID_FILE).unlink()
        print("FST stopped.")
    except Exception as e:
        print(f"Error stopping FST: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="fst - Real-time dbt DuckDB query runner."
    )
    subparsers = parser.add_subparsers()

    start_parser = subparsers.add_parser("start", help="Start fst.")
    start_parser.set_defaults(func=start)

    stop_parser = subparsers.add_parser("stop", help="Stop fst.")
    stop_parser.set_defaults(func=stop)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
