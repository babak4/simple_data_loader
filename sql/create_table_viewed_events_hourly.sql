CREATE TABLE IF NOT EXISTS viewed_events_hourly(
  view_date_hour    TIMESTAMP,
  event_name        VARCHAR,
  view_source       VARCHAR,
  viewers_total     INTEGER,
  etl_staging_time  TIMESTAMP,
  etl_load_time     TIMESTAMP
)
