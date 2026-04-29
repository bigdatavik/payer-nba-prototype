-- Bronze layer: Raw interactions data ingestion
CREATE OR REFRESH STREAMING TABLE bronze_interactions
COMMENT 'Raw member interactions data ingested from parquet files'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true',
  'contains_pii' = 'true'
)
AS SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw_data/interactions',
  format => 'parquet'
);
