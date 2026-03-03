\# FAERS Mini Lakehouse



Small end-to-end data engineering pipeline using the FDA openFDA (FAERS) adverse event API.



\## What it does

\- Ingests FAERS adverse event reports from openFDA (JSONL landing file)

\- Bronze: converts raw JSONL to a queryable Parquet table + ingestion metadata

\- Silver: normalizes nested JSON into relational-style tables (`events`, `drugs`, `reactions`)

\- Gold: produces analytics aggregates (top reactions + seriousness rate)

\- Orchestration: runs the full pipeline with one command



\## Architecture

`API → JSONL → Bronze → Silver → Gold`



\## Run

```bash

pip install -r requirements.txt

python src/run\_pipeline.py





