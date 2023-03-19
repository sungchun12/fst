import argparse
from fst import fst_query

def main():
    parser = argparse.ArgumentParser(description="Real-time query execution for FST.")
    parser.add_argument("action", choices=["start", "stop", "restart"], help="Action to perform: start, stop, or restart.")
    parser.add_argument("--file-path", type=str, help="Path to the SQL file you want to watch.")

    args = parser.parse_args()

    if args.action == "start":
        if args.file_path:
            fst_query.watch_directory(fst_query.CURRENT_WORKING_DIR, fst_query.handle_query, args.file_path)
        else:
            print("Please provide a file path using the --file-path option.")
    elif args.action == "stop":
        if fst_query.observer:
            fst_query.observer.stop()
            fst_query.observer.join()
            print("Stopped watching the directory.")
        else:
            print("No observer is currently running.")
    elif args.action == "restart":
        if fst_query.observer:
            fst_query.observer.stop()
            fst_query.observer.join()
            print("Stopped watching the directory.")
        else:
            print("No observer was running. Starting a new one.")
        
        if args.file_path:
            fst_query.watch_directory(fst_query.CURRENT_WORKING_DIR, fst_query.handle_query, args.file_path)
        else:
            print("Please provide a file path using the --file-path option.")

if __name__ == "__main__":
    main()
