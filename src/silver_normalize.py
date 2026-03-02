import json
from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
SILVER_DIR = Path("data/silver")
SILVER_DIR.mkdir(parents=True, exist_ok=True)

def find_latest_jsonl(raw_dir: Path) -> Path:
    files = sorted(raw_dir.glob("faers_raw_*.jsonl"))
    if not files:
        raise FileNotFoundError(f"No faers_raw_*.jsonl found in {raw_dir}")
    return files[-1]

def load_raw_records(path: Path):
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            records.append(json.loads(line))
    return records

def build_events(records):
    rows = []
    for r in records:
        patient = r.get("patient", {}) or {}
        rows.append({
            "safetyreportid": r.get("safetyreportid"),
            "safetyreportversion": r.get("safetyreportversion"),
            "receiptdate": r.get("receiptdate"),
            "receivedate": r.get("receivedate"),
            "transmissiondate": r.get("transmissiondate"),
            "primarysourcecountry": r.get("primarysourcecountry"),
            "occurcountry": r.get("occurcountry"),
            "reporttype": r.get("reporttype"),
            "serious": r.get("serious"),
            "companynumb": r.get("companynumb"),
            "duplicate": r.get("duplicate"),
            "_ingested_at_utc": r.get("_ingested_at_utc"),
            "patientonsetage": patient.get("patientonsetage"),
            "patientonsetageunit": patient.get("patientonsetageunit"),
            "patientsex": patient.get("patientsex"),
        })
    return pd.DataFrame(rows)

def build_drugs(records):
    rows = []
    for r in records:
        sid = r.get("safetyreportid")
        patient = r.get("patient", {}) or {}
        drugs = patient.get("drug", []) or []
        for d in drugs:
            rows.append({
                "safetyreportid": sid,
                "medicinalproduct": d.get("medicinalproduct"),
                "drugcharacterization": d.get("drugcharacterization"),
                "drugindication": d.get("drugindication"),
                "drugadministrationroute": d.get("drugadministrationroute"),
                "drugauthorizationnumb": d.get("drugauthorizationnumb"),
                "drugstructuredosagenumb": d.get("drugstructuredosagenumb"),
                "drugstructuredosageunit": d.get("drugstructuredosageunit"),
                "drugdosageform": d.get("drugdosageform"),
                "drugstartdate": d.get("drugstartdate"),
                "drugenddate": d.get("drugenddate"),
                "_ingested_at_utc": r.get("_ingested_at_utc"),
            })
    return pd.DataFrame(rows)

def build_reactions(records):
    rows = []
    for r in records:
        sid = r.get("safetyreportid")
        patient = r.get("patient", {}) or {}
        reactions = patient.get("reaction", []) or []
        for rx in reactions:
            rows.append({
                "safetyreportid": sid,
                "reactionmeddrapt": rx.get("reactionmeddrapt"),
                "reactionoutcome": rx.get("reactionoutcome"),
                "_ingested_at_utc": r.get("_ingested_at_utc"),
            })
    return pd.DataFrame(rows)

def quality_checks(events_df, drugs_df, reactions_df):
    assert events_df["safetyreportid"].notna().all()
    assert events_df["safetyreportid"].is_unique
    assert len(drugs_df) >= len(events_df)
    assert len(reactions_df) >= len(events_df)

def main():
    raw_path = find_latest_jsonl(RAW_DIR)
    records = load_raw_records(raw_path)

    events_df = build_events(records)
    drugs_df = build_drugs(records)
    reactions_df = build_reactions(records)

    quality_checks(events_df, drugs_df, reactions_df)

    events_path = SILVER_DIR / "silver_events.parquet"
    drugs_path = SILVER_DIR / "silver_drugs.parquet"
    reactions_path = SILVER_DIR / "silver_reactions.parquet"

    events_df.to_parquet(events_path, index=False)
    drugs_df.to_parquet(drugs_path, index=False)
    reactions_df.to_parquet(reactions_path, index=False)

    print("Silver tables created:")
    print("Events:", len(events_df))
    print("Drugs:", len(drugs_df))
    print("Reactions:", len(reactions_df))

if __name__ == "__main__":
    main()