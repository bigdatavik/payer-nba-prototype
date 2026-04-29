-- Bronze layer: Raw digital events data ingestion
CREATE OR REFRESH STREAMING TABLE bronze_digital_events
COMMENT 'Raw digital engagement events data ingested from parquet files'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw_data/digital_events',
  format => 'parquet'
);
