import os

print("Processing Service gestartet")
print("DATA_DIR:", os.getenv("DATA_DIR", "/data"))
print("POSTGRES_HOST:", os.getenv("POSTGRES_HOST", "postgres"))
print("TODO: DuckDB Verarbeitung + Aggregationen + Schreiben in Postgres/Processed Layer")