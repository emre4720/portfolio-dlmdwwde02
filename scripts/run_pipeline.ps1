docker compose up -d postgres
docker compose run --rm ingestion
docker compose run --rm processing
docker compose run --rm aggregation