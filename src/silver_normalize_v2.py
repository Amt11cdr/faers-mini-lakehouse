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

def make_event_key(r: dict) -> str:
    sid = r.get("safetyreportid")
    ver = r.get("safetyreportversion")
    return f"{sid}:{ver}" if ver is not None else f"{sid}:NA"

def build_events(records):
    rows = []
    for r in records:
        patient = r.get("patient", {}) or {}
        sid = r.get("safetyreportid")
        ver = r.get("safetyreportversion")
        rows.append({
            "event_key": make_event_key(r),
            "safetyreportid": sid,
            "safetyreportversion": ver,
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
    df = pd.DataFrame(rows)

    # De-duplicate on event_key (keep first occurrence within this batch)
    df = df.drop_duplicates(subset=["event_key"], keep="first")

    return df

def build_drugs(records):
    rows = []
    for r in records:
        ek = make_event_key(r)
        sid = r.get("safetyreportid")
        patient = r.get("patient", {}) or {}
        drugs = patient.get("drug", []) or []
        for d in drugs:
            rows.append({
                "event_key": ek,
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
        ek = make_event_key(r)
        sid = r.get("safetyreportid")
        patient = r.get("patient", {}) or {}
        reactions = patient.get("reaction", []) or []
        for rx in reactions:
            rows.append({
                "event_key": ek,
                "safetyreportid": sid,
                "reactionmeddrapt": rx.get("reactionmeddrapt"),
                "reactionoutcome": rx.get("reactionoutcome"),
                "_ingested_at_utc": r.get("_ingested_at_utc"),
            })
    return pd.DataFrame(rows)

def quality_checks(events_df, drugs_df, reactions_df):
    assert events_df["event_key"].notna().all(), "Null event_key in events"
    assert events_df["event_key"].is_unique, "event_key is not unique in events"

    # child rows must reference known events
    assert drugs_df["event_key"].isin(events_df["event_key"]).all(), "drugs has unknown event_key"
    assert reactions_df["event_key"].isin(events_df["event_key"]).all(), "reactions has unknown event_key"

    assert len(drugs_df) >= len(events_df), "drugs rows < events rows (unexpected)"
    assert len(reactions_df) >= len(events_df), "reactions rows < events rows (unexpected)"

def main():
    raw_path = find_latest_jsonl(RAW_DIR)
    records = load_raw_records(raw_path)

    events_df = build_events(records)
    drugs_df = build_drugs(records)
    reactions_df = build_reactions(records)

    # Filter child tables to match de-duped event set
    valid_keys = set(events_df["event_key"].tolist())
    drugs_df = drugs_df[drugs_df["event_key"].isin(valid_keys)].copy()
    reactions_df = reactions_df[reactions_df["event_key"].isin(valid_keys)].copy()

    quality_checks(events_df, drugs_df, reactions_df)

    events_path = SILVER_DIR / "silver_events.parquet"
    drugs_path = SILVER_DIR / "silver_drugs.parquet"
    reactions_path = SILVER_DIR / "silver_reactions.parquet"

    events_df.to_parquet(events_path, index=False)
    drugs_df.to_parquet(drugs_path, index=False)
    reactions_df.to_parquet(reactions_path, index=False)

    print(f"? Read raw file: {raw_path}")
    print(f"? Wrote: {events_path} | rows={len(events_df)} cols={len(events_df.columns)}")
    print(f"? Wrote: {drugs_path}  | rows={len(drugs_df)} cols={len(drugs_df.columns)}")
    print(f"? Wrote: {reactions_path} | rows={len(reactions_df)} cols={len(reactions_df.columns)}")

    print()
    print("Top 10 drugs by count:")
    if "medicinalproduct" in drugs_df.columns and len(drugs_df) > 0:
        print(drugs_df["medicinalproduct"].value_counts().head(10))
    else:
        print("(no drug rows)")

    print()
    print("Top 10 reactions by count:")
    if "reactionmeddrapt" in reactions_df.columns and len(reactions_df) > 0:
        print(reactions_df["reactionmeddrapt"].value_counts().head(10))
    else:
        print("(no reaction rows)")

if __name__ == "__main__":
    main()
