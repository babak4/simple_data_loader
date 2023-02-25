CREATE OR REPLACE TEMP TABLE tmp_hourly_aggregates AS
    SELECT
        properties['event_name'] as event_name
        , source
        , count(*) as viewers_total
        , current_timestamp as etl_staging_time
    FROM READ_NDJSON_AUTO('{{ data_dir }}/*.json')
    WHERE properties['type'] = 'event_viewed'
    GROUP BY
        properties['event_name']
        , source
