import subprocess
import sys

STEPS = [
    ("Ingest", ["python", "src/openfda_client.py"]),
    ("Bronze", ["python", "src/bronze_build.py"]),
    ("Silver", ["python", "src/silver_normalize_v2.py"]),
    ("Gold", ["python", "src/gold_aggregates.py"]),
]

def main():
    for name, cmd in STEPS:
        print(f"\n=== Running: {name} ===")
        result = subprocess.run(cmd)

        if result.returncode != 0:
            print(f"\nPipeline failed at step: {name}")
            sys.exit(1)

    print("\nPipeline finished successfully.")

if __name__ == "__main__":
    main()