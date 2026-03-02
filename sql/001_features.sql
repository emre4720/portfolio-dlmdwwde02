CREATE TABLE IF NOT EXISTS features_airline_quarter (
  year INT,
  quarter INT,
  airline TEXT,
  flights INT,
  avg_delay_minutes DOUBLE PRECISION,
  cancel_rate DOUBLE PRECISION
);