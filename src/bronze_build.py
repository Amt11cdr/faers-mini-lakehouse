import json
from pathlib import Path
from datetime import datetime

import pandas as pd

RAW_DIR = Path("data/raw")
OUT_DIR = Path("data/bronze")
OUT_FILE = OUT_DIR / "bronze_faers_raw.parquet"

def find_latest_jsonl(raw_dir: Path) -> Path:
    files = sorted(raw_dir.glob("faers_raw_*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No faers_raw_*.jsonl found in {raw_dir}")
    return files[-1]

def read_jsonl(path: Path) -> pd.DataFrame:
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            records.append(json.loads(line))
    return pd.json_normalize(records)

def main():
    raw_path = find_latest_jsonl(RAW_DIR)
    df = read_jsonl(raw_path)

    # Minimal pipeline metadata (table-level)
    df["ingest_date"] = datetime.utcnow().strftime("%Y-%m-%d")
    df["source"] = "openfda_faers"
    df["raw_filename"] = raw_path.name

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_FILE, index=False)

    print(f"? Read raw file: {raw_path}")
    print(f"? Bronze parquet written: {OUT_FILE}")
    print(f"Rows: {len(df)} | Columns: {len(df.columns)}")
    print("Sample columns:")
    print(df.columns[:25].tolist())

if __name__ == "__main__":
    main()
