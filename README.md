FAERS Mini Lakehouse

End-to-End Data Engineering Pipeline (Bronze / Silver / Gold + Orchestration)



Overview



This project implements a small but production-style data engineering pipeline using real public healthcare data from the FDA openFDA Drug Adverse Event (FAERS) API.



The pipeline:



Ingests adverse drug event data from a public API



Stores raw data in a landing layer



Normalizes nested JSON into relational-style tables



Produces analytics-ready aggregates



Runs end-to-end via an orchestration script



The goal is to simulate a miniature healthcare data platform using medallion architecture.



Data Source



API: https://api.fda.gov/drug/event.json



Dataset: FAERS (FDA Adverse Event Reporting System)



Example drugs queried:



Metformin



Ibuprofen



Amoxicillin



Each run pulls 100 reports per drug.



Architecture



Medallion pattern:



API → JSONL → Bronze → Silver → Gold



Bronze



Raw JSONL converted to Parquet



Ingestion metadata added



Schema discovery preserved



Silver

Nested JSON flattened into relational-style tables:



silver\_events (1 row per event\_key)



silver\_drugs (exploded drug records)



silver\_reactions (exploded reaction records)



Composite key used:

event\_key = safetyreportid : safetyreportversion



Gold

Analytics-ready aggregates:



Top reactions per drug



Seriousness rate per drug



Sample outputs:



data/sample\_outputs/top\_reactions\_by\_drug.csv



data/sample\_outputs/seriousness\_rate\_by\_drug.csv



Orchestration



The pipeline can be executed end-to-end:



python src/run\_pipeline.py



This sequentially runs:



API ingestion



Bronze layer build



Silver normalization



Gold aggregation



If any step fails, execution stops.



Tech Stack



Python



pandas



pyarrow



requests



Git



Repository Structure



faers-mini-lakehouse/

│

├── src/

│ ├── openfda\_client.py

│ ├── bronze\_build.py

│ ├── silver\_normalize\_v2.py

│ ├── gold\_aggregates.py

│ └── run\_pipeline.py

│

├── data/

│ └── sample\_outputs/

│

├── requirements.txt

└── README.md



Generated data layers (raw/bronze/silver/gold) are excluded via .gitignore.



Key Engineering Concepts Demonstrated



API-based ingestion



Handling nested JSON



Relational normalization



Medallion architecture



Basic data quality checks



Deterministic orchestration



Git repository hygiene



Possible Extensions



Convert Parquet layers to Delta Lake (Databricks)



Add incremental ingestion by date window



Partition Bronze by ingestion date



Introduce a data quality framework



Add CI pipeline



Why This Project



This project demonstrates the core components of a modern data pipeline using real healthcare data, scaled down to a reproducible local environment.



It mirrors real-world architecture patterns used in healthcare and pharma analytics systems.

