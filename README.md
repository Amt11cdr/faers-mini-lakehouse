\# FAERS Mini Lakehouse



End-to-end data engineering pipeline built on FDA openFDA (FAERS) adverse event data.



---



\## Architecture



API → JSONL → Bronze → Silver → Gold



\- \*\*Bronze\*\*: Raw JSONL converted to Parquet with ingestion metadata  

\- \*\*Silver\*\*: Nested JSON normalized into relational-style tables  

\- \*\*Gold\*\*: Analytics-ready aggregates  



---



\## Tables (Silver Layer)



\- `silver\_events` – 1 row per event\_key  

\- `silver\_drugs` – exploded drug records  

\- `silver\_reactions` – exploded reaction records  



Composite key used:



event\_key = safetyreportid:safetyreportversion



---



\## Outputs (Gold Layer)



Generated sample analytics:



\- `data/sample\_outputs/top\_reactions\_by\_drug.csv`

\- `data/sample\_outputs/seriousness\_rate\_by\_drug.csv`



These contain:

\- Most frequent adverse reactions per drug

\- Seriousness rate per drug



---



\## Run

pip install -r requirements.txt

python src/run\_pipeline.py



---



\## Notes



\- Built using real public healthcare regulatory data  

\- Generated data layers (raw/bronze/silver/gold) are excluded via `.gitignore`  

\- Demonstrates ingestion, normalization, aggregation, and orchestration  

