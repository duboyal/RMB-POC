import shutil
import time
import traceback
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from importer import import_file

INCOMING = Path("/data/incoming")
PROCESSING = Path("/data/processing")
PROCESSED = Path("/data/processed")
ERROR = Path("/data/error")
LOGS = Path("/data/logs")


def ensure_directories() -> None:
    for d in [INCOMING, PROCESSING, PROCESSED, ERROR, LOGS]:
        d.mkdir(parents=True, exist_ok=True)
        print(f"Ensured directory exists: {d}", flush=True)


def remove_if_exists(path: Path) -> None:
    if path.exists():
        if path.is_file():
            path.unlink()
            print(f"Removed stale file: {path}", flush=True)
        else:
            raise ValueError(f"Expected file but found directory: {path}")


class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        src = Path(event.src_path)
        print(f"Detected new file: {src}", flush=True)

        if src.suffix != ".ready":
            print(f"Skipping non-.ready file: {src.name}", flush=True)
            return

        time.sleep(1)

        if not src.exists():
            print(f"Source file no longer exists, skipping: {src}", flush=True)
            return

        proc = PROCESSING / src.name
        processed_dest = PROCESSED / src.name
        error_dest = ERROR / src.name

        try:
            remove_if_exists(proc)
            print(f"Moving {src.name} -> {proc}", flush=True)
            shutil.move(src, proc)

            row_count = import_file(proc)

            remove_if_exists(processed_dest)
            shutil.move(proc, processed_dest)
            print(f"Processed {proc.name} ({row_count} rows)", flush=True)

        except Exception as e:
            print(f"Error {src.name}: {e}", flush=True)
            traceback.print_exc()

            try:
                if proc.exists():
                    remove_if_exists(error_dest)
                    shutil.move(proc, error_dest)
                    print(f"Moved {proc.name} to error folder", flush=True)
                else:
                    print(
                        f"Could not move {src.name} to error folder because processing file does not exist.",
                        flush=True,
                    )
            except Exception as move_error:
                print(
                    f"Failed while moving errored file to error folder: {move_error}",
                    flush=True,
                )
                traceback.print_exc()


if __name__ == "__main__":
    ensure_directories()

    observer = Observer()
    observer.schedule(Handler(), str(INCOMING), recursive=False)
    observer.start()

    print("Watching...", flush=True)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
