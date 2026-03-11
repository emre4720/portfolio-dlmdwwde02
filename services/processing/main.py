import os
from pathlib import Path
import pandas as pd

DATA_DIR = os.getenv("DATA_DIR", "/data")
RAW_DIR = Path(DATA_DIR) / "raw"
PROCESSED_DIR = Path(DATA_DIR) / "processed"

RAW_FILE = RAW_DIR / "flights.csv"
REPORT_FILE = PROCESSED_DIR / "processing_report.txt"

# Erwartete Spalten (für Demo/Standardfall)
EXPECTED_COLS = ["flight_date", "airline", "origin", "dest", "dep_delay", "cancelled"]


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append("Processing Service gestartet")
    lines.append(f"DATA_DIR={DATA_DIR}")
    lines.append(f"RAW_FILE={RAW_FILE}")

    if not RAW_FILE.exists():
        lines.append("STATUS=SKIPPED")
        lines.append("REASON=Raw-Datei nicht gefunden. Bitte zuerst Ingestion oder Aggregation (Demo-CSV) ausführen.")
        REPORT_FILE.write_text("\n".join(lines), encoding="utf-8")
        print("\n".join(lines))
        return

    # Nur kleine Stichprobe laden (schnell), reicht als Validierung
    df = pd.read_csv(RAW_FILE, nrows=5000)

    cols = [c.strip() for c in df.columns.tolist()]
    missing = [c for c in EXPECTED_COLS if c not in cols]

    lines.append(f"ROWS_SAMPLE={len(df)}")
    lines.append(f"COLUMNS={cols}")

    if missing:
        lines.append("STATUS=WARNING")
        lines.append(f"MISSING_COLUMNS={missing}")
        lines.append("NOTE=Die Datei wird nicht verändert. Spaltennamen ggf. an Kaggle-Datensatz anpassen.")
    else:
        lines.append("STATUS=OK")
        # Timestamp-Check
        try:
            ts = pd.to_datetime(df["flight_date"], errors="coerce")
            invalid_ts = int(ts.isna().sum())
            lines.append(f"TIMESTAMP_INVALID={invalid_ts}")
        except Exception as e:
            lines.append("STATUS=WARNING")
            lines.append(f"TIMESTAMP_PARSE_ERROR={e}")

        # Beispiel: Minimaler Datencheck
        nulls = df[EXPECTED_COLS].isna().sum().to_dict()
        lines.append(f"NULL_COUNTS={nulls}")

    # Report schreiben (Nachweis für Prof)
    REPORT_FILE.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f" Report geschrieben: {REPORT_FILE}")


if __name__ == "__main__":
    main()