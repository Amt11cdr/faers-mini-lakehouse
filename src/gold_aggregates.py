from pathlib import Path
import pandas as pd

SILVER_DIR = Path("data/silver")
GOLD_DIR = Path("data/gold")
EXPORT_DIR = Path("data/sample_outputs")

GOLD_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

EVENTS_PATH = SILVER_DIR / "silver_events.parquet"
DRUGS_PATH = SILVER_DIR / "silver_drugs.parquet"
REACTIONS_PATH = SILVER_DIR / "silver_reactions.parquet"

def main():
    events = pd.read_parquet(EVENTS_PATH)
    drugs = pd.read_parquet(DRUGS_PATH)
    reactions = pd.read_parquet(REACTIONS_PATH)

    drugs["medicinalproduct"] = drugs["medicinalproduct"].astype(str).str.strip().str.upper()
    reactions["reactionmeddrapt"] = reactions["reactionmeddrapt"].astype(str).str.strip()

    focus = {"METFORMIN", "IBUPROFEN", "AMOXICILLIN"}
    focus_drugs = drugs[drugs["medicinalproduct"].isin(focus)][["event_key", "medicinalproduct"]].drop_duplicates()

    drug_rx = focus_drugs.merge(
        reactions[["event_key", "reactionmeddrapt"]],
        on="event_key",
        how="inner"
    )

    top_rx = (
        drug_rx.groupby(["medicinalproduct", "reactionmeddrapt"])
               .size()
               .reset_index(name="count")
               .sort_values(["medicinalproduct", "count"], ascending=[True, False])
    )

    top_rx["rank"] = top_rx.groupby("medicinalproduct")["count"].rank(method="first", ascending=False)
    top_rx_15 = top_rx[top_rx["rank"] <= 15].drop(columns=["rank"]).reset_index(drop=True)

    gold1_path = GOLD_DIR / "gold_top_reactions_by_drug.parquet"
    top_rx_15.to_parquet(gold1_path, index=False)

    csv1 = EXPORT_DIR / "top_reactions_by_drug.csv"
    top_rx_15.to_csv(csv1, index=False)

    ev = events[["event_key", "serious"]].copy()
    ev["serious_flag"] = (ev["serious"].astype(str).str.strip() == "1").astype(int)

    drug_ser = focus_drugs.merge(ev[["event_key", "serious_flag"]], on="event_key", how="left")

    serious_rate = (
        drug_ser.groupby("medicinalproduct")
                .agg(
                    reports=("event_key", "nunique"),
                    serious_reports=("serious_flag", "sum"),
                )
                .reset_index()
    )

    serious_rate["serious_rate"] = (serious_rate["serious_reports"] / serious_rate["reports"]).round(4)

    gold2_path = GOLD_DIR / "gold_seriousness_rate_by_drug.parquet"
    serious_rate.to_parquet(gold2_path, index=False)

    csv2 = EXPORT_DIR / "seriousness_rate_by_drug.csv"
    serious_rate.to_csv(csv2, index=False)

    print("Gold tables created:")
    print(gold1_path)
    print(gold2_path)
    print()
    print("Seriousness rate:")
    print(serious_rate.sort_values("serious_rate", ascending=False))

if __name__ == "__main__":
    main()