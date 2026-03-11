\## Pipeline ausführen (lokal)



\### Orchestrierung (Scope-Erweiterung)

PowerShell:

.\\scripts\\run\_pipeline.ps1



1\) Postgres starten:

&#x20;  docker compose up -d postgres



2\) Processing (Validierung) ausführen:

&#x20;  docker compose run --rm processing



3\) Aggregation (DuckDB) ausführen:

&#x20;  docker compose run --rm aggregation



\## Outputs



\- Processed Layer: /data/processed/features\_airline\_quarter.parquet

\- Serving Store (PostgreSQL): Tabelle features\_airline\_quarter

\- Processing Report: /data/processed/processing\_report.txt



\## Verifikation



\- Tabellen anzeigen:

&#x20; docker compose exec -T postgres psql -U portfolio -d portfolio -c "\\dt"



\- Daten prüfen:

&#x20; docker compose exec -T postgres psql -U portfolio -d portfolio -c "SELECT \* FROM features\_airline\_quarter LIMIT 5;"

