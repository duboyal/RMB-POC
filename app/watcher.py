from __future__ import annotations

import shutil
import time
import traceback
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from app.duckdb_pipeline.jobs.cust1_job import run_cust1_job
from app.duckdb_pipeline.jobs.detal1_job import run_detal1_job
from app.duckdb_pipeline.jobs.heder1_job import run_heder1_job
from app.duckdb_pipeline.jobs.inven1_job import run_inven1_job
from app.duckdb_pipeline.jobs.oitem1_job import run_oitem1_job
from app.duckdb_pipeline.jobs.pcode1_job import run_pcode1_job
from app.duckdb_pipeline.jobs.scode1_job import run_scode1_job
from app.duckdb_pipeline.jobs.shipt1_job import run_shipt1_job
from app.duckdb_pipeline.jobs.whsfl1_job import run_whsfl1_job


INCOMING = Path("/data/incoming")
PROCESSING = Path("/data/processing")
PROCESSED = Path("/data/processed")
ERROR = Path("/data/error")
LOGS = Path("/data/logs")


READY_TO_DATA_MAP = {
    "cust1.ready": "cust1.txt",
    "detal1.ready": "detal1.txt",
    "heder1.ready": "heder1.txt",
    "inven1.ready": "inven1.txt",
    "oitem1.ready": "oitem1.txt",
    "pcode1.ready": "pcode1.txt",
    "scode1.ready": "scode1.txt",
    "shipt1.ready": "shipt1.txt",
    "whsfl1.ready": "whsfl1.txt",
}


JOB_MAP = {
    "cust1.ready": run_cust1_job,
    "detal1.ready": run_detal1_job,
    "heder1.ready": run_heder1_job,
    "inven1.ready": run_inven1_job,
    "oitem1.ready": run_oitem1_job,
    "pcode1.ready": run_pcode1_job,
    "scode1.ready": run_scode1_job,
    "shipt1.ready": run_shipt1_job,
    "whsfl1.ready": run_whsfl1_job,
}


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


def get_data_file_for_ready(ready_file: Path) -> Path:
    data_name = READY_TO_DATA_MAP.get(ready_file.name.lower())

    if not data_name:
        raise ValueError(
            f"No data-file mapping configured for ready file: {ready_file.name}"
        )

    return ready_file.parent / data_name


def process_ready_file(ready_file: Path) -> None:
    print(f"Detected ready file: {ready_file}", flush=True)

    if ready_file.suffix.lower() != ".ready":
        print(f"Skipping non-.ready file: {ready_file.name}", flush=True)
        return

    time.sleep(1)

    if not ready_file.exists():
        print(f"Ready file no longer exists, skipping: {ready_file}", flush=True)
        return

    data_file = get_data_file_for_ready(ready_file)

    if not data_file.exists():
        raise FileNotFoundError(
            f"Ready file {ready_file.name} found, but matching data file does not exist: {data_file.name}"
        )

    proc_ready = PROCESSING / ready_file.name
    proc_data = PROCESSING / data_file.name

    processed_ready = PROCESSED / ready_file.name
    processed_data = PROCESSED / data_file.name

    error_ready = ERROR / ready_file.name
    error_data = ERROR / data_file.name

    try:
        remove_if_exists(proc_ready)
        remove_if_exists(proc_data)

        print(f"Moving {ready_file.name} -> {proc_ready}", flush=True)
        shutil.move(ready_file, proc_ready)

        print(f"Moving {data_file.name} -> {proc_data}", flush=True)
        shutil.move(data_file, proc_data)

        job = JOB_MAP.get(proc_ready.name.lower())

        if not job:
            raise ValueError(f"No job configured for ready file: {proc_ready.name}")

        print(f"Running job for {proc_ready.name} on {proc_data}", flush=True)
        job(str(proc_data))

        remove_if_exists(processed_ready)
        remove_if_exists(processed_data)

        shutil.move(proc_ready, processed_ready)
        shutil.move(proc_data, processed_data)

        print(f"Processed {proc_data.name}", flush=True)

    except Exception as exc:
        print(f"Error processing {ready_file.name}: {exc}", flush=True)
        traceback.print_exc()

        try:
            if proc_ready.exists():
                remove_if_exists(error_ready)
                shutil.move(proc_ready, error_ready)
                print(f"Moved ready file to error folder: {proc_ready.name}", flush=True)

            if proc_data.exists():
                remove_if_exists(error_data)
                shutil.move(proc_data, error_data)
                print(f"Moved data file to error folder: {proc_data.name}", flush=True)

        except Exception as move_error:
            print(f"Failed while moving errored files: {move_error}", flush=True)
            traceback.print_exc()


class Handler(FileSystemEventHandler):
    def on_created(self, event) -> None:
        if event.is_directory:
            return

        process_ready_file(Path(event.src_path))


def process_existing_ready_files() -> None:
    if not INCOMING.exists():
        return

    for path in INCOMING.iterdir():
        if path.is_file() and path.suffix.lower() == ".ready":
            try:
                process_ready_file(path)
            except Exception as exc:
                print(f"Startup processing error for {path.name}: {exc}", flush=True)
                traceback.print_exc()


if __name__ == "__main__":
    ensure_directories()
    process_existing_ready_files()

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