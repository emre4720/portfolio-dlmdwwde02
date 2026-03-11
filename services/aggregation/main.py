import os
import duckdb
import pandas as pd
from sqlalchemy import create_engine

DATA_DIR = os.getenv("DATA_DIR", "/data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")

RAW_FILE = os.path.join(RAW_DIR, "flights.csv")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "portfolio")
PG_USER = os.getenv("POSTGRES_USER", "portfolio")
PG_PW = os.getenv("POSTGRES_PASSWORD", "portfolio")


def ensure_dirs():
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)


def create_demo_raw():
    # Demo-Daten (klar gekennzeichnet), falls noch kein Kaggle-Import läuft
    demo = pd.DataFrame(
        {
            "flight_date": ["2024-01-15", "2024-02-20", "2024-04-02", "2024-04-15"],
            "airline": ["AA", "AA", "LH", "LH"],
            "origin": ["JFK", "JFK", "FRA", "FRA"],
            "dest": ["LAX", "MIA", "HAM", "MUC"],
            "dep_delay": [10, 55, 0, 25],
            "cancelled": [0, 0, 0, 1],
        }
    )
    demo.to_csv(RAW_FILE, index=False)
    print(f" Keine Raw-Datei gefunden – Demo-Daten erzeugt: {RAW_FILE}")


def main():
    ensure_dirs()

    if not os.path.exists(RAW_FILE):
        create_demo_raw()

    con = duckdb.connect()

    con.execute(f"""
        CREATE OR REPLACE VIEW flights AS
        SELECT
            CAST(flight_date AS DATE) AS flight_date,
            airline,
            origin,
            dest,
            CAST(COALESCE(dep_delay, 0) AS DOUBLE) AS dep_delay,
            CAST(COALESCE(cancelled, 0) AS DOUBLE) AS cancelled
        FROM read_csv_auto('{RAW_FILE}')
    """)

    con.execute("""
        CREATE OR REPLACE VIEW flights_q AS
        SELECT
            EXTRACT(YEAR FROM flight_date)::INT AS year,
            ((EXTRACT(MONTH FROM flight_date)-1)/3 + 1)::INT AS quarter,
            airline, origin, dest,
            dep_delay, cancelled
        FROM flights
    """)

    df_airline = con.execute("""
        SELECT
            year, quarter, airline,
            COUNT(*)::INT AS flights,
            AVG(dep_delay) AS avg_dep_delay,
            AVG(cancelled) AS cancel_rate
        FROM flights_q
        GROUP BY 1,2,3
        ORDER BY 1,2,3
    """).df()

    out_parquet = os.path.join(PROCESSED_DIR, "features_airline_quarter.parquet")
    df_airline.to_parquet(out_parquet, index=False)

    # Optional: in PostgreSQL (Serving Store)
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{PG_USER}:{PG_PW}@{PG_HOST}:5432/{PG_DB}"
        )
        df_airline.to_sql("features_airline_quarter", engine, if_exists="replace", index=False)
        print(" Postgres Tabelle geschrieben: features_airline_quarter")
    except Exception as e:
        print(" Postgres nicht erreichbar/konfiguriert – DB-Write übersprungen.")
        print(f"   Details: {e}")

    print(" Aggregation fertig")
    print(f"→ Processed: {out_parquet}")


if __name__ == "__main__":
    main()