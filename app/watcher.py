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

        proc = PROCESSING / src.name
        print(f"Moving {src.name} -> {proc}", flush=True)
        shutil.move(src, proc)

        try:
            row_count = import_file(proc)
            shutil.move(proc, PROCESSED / proc.name)
            print(f"Processed {proc.name} ({row_count} rows)", flush=True)
        except Exception as e:
            print(f"Error {proc.name}: {e}", flush=True)
            traceback.print_exc()
            shutil.move(proc, ERROR / proc.name)
            print(f"Moved {proc.name} to error folder", flush=True)


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
