import os

print("Ingestion Service gestartet")
print("KAGGLE_USERNAME gesetzt:", bool(os.getenv("KAGGLE_USERNAME")))
print("DATA_DIR:", os.getenv("DATA_DIR", "/data"))
print("TODO: Kaggle Download + Schema-Check + Raw-Ablage")