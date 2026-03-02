import requests
import json
from datetime import datetime
from pathlib import Path

BASE_URL = "https://api.fda.gov/drug/event.json"

def fetch_drug_events(drug_name: str, limit: int = 100):
    query = f'search=patient.drug.medicinalproduct:"{drug_name}"&limit={limit}'
    url = f"{BASE_URL}?{query}"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    results = data.get("results", [])
    return results, url

def save_as_jsonl(records, output_path: Path):
    ingestion_time = datetime.utcnow().isoformat()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        for record in records:
            record["_ingested_at_utc"] = ingestion_time
            f.write(json.dumps(record) + "\n")

def main():
    drugs = ["metformin", "ibuprofen", "amoxicillin"]
    per_drug_limit = 100

    all_records = []
    urls_used = []

    for drug in drugs:
        records, url = fetch_drug_events(drug, limit=per_drug_limit)
        all_records.extend(records)
        urls_used.append((drug, url, len(records)))

    today = datetime.utcnow().strftime("%Y%m%d")
    output_file = Path(f"data/raw/faers_raw_{today}.jsonl")

    save_as_jsonl(all_records, output_file)

    print(f"\nWrote {len(all_records)} total records to: {output_file}\n")
    print("Query summary:")
    for drug, url, n in urls_used:
        print(f"- {drug}: {n} records")
        print(f"  {url}")

if __name__ == "__main__":
    main()
