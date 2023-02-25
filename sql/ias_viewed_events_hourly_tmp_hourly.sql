INSERT INTO viewed_events_hourly (
  view_date_hour
  , event_name
  , view_source
  , viewers_total
  , etl_staging_time
  , etl_load_time
)
  SELECT
    '{{ date_hour }}'::TIMESTAMP
    , event_name
    , source
    , viewers_total
    , etl_staging_time
    , current_timestamp as etl_load_time
  FROM tmp_hourly_aggregates
