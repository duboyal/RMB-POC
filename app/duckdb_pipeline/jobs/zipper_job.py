def run_zipper_job(file_path: str) -> None:
    print(f"[zipper] Processing file: {file_path}", flush=True)

    with open(file_path, "r", encoding="utf-8-sig", errors="replace") as f:
        row_count = sum(1 for _ in f)

    print(f"[zipper] Row count: {row_count}", flush=True)
    print("[zipper] Done.", flush=True)