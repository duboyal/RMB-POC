import time
import shutil
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from importer import import_file

INCOMING = Path("/data/incoming")
PROCESSING = Path("/data/processing")
PROCESSED = Path("/data/processed")
ERROR = Path("/data/error")

for d in [INCOMING, PROCESSING, PROCESSED, ERROR]:
    d.mkdir(parents=True, exist_ok=True)


class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        src = Path(event.src_path)

        if src.suffix != ".ready":
            return

        time.sleep(1)

        proc = PROCESSING / src.name
        shutil.move(src, proc)

        try:
            import_file(proc)
            shutil.move(proc, PROCESSED / proc.name)
            print(f"Processed {proc.name}")
        except Exception as e:
            shutil.move(proc, ERROR / proc.name)
            print(f"Error {proc.name}: {e}")


if __name__ == "__main__":
    observer = Observer()
    observer.schedule(Handler(), str(INCOMING), recursive=False)
    observer.start()

    print("Watching...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
