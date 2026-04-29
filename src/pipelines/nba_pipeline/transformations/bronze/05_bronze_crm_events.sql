-- Bronze layer: Raw CRM events data ingestion
CREATE OR REFRESH STREAMING TABLE bronze_crm_events
COMMENT 'Raw CRM/marketing events data ingested from parquet files'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw_data/crm_events',
  format => 'parquet'
);
