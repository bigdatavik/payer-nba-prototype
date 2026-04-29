-- Bronze layer: Raw policy data ingestion
CREATE OR REFRESH STREAMING TABLE bronze_policies
COMMENT 'Raw policy data ingested from parquet files'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true',
  'contains_pii' = 'false'
)
AS SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw_data/policies',
  format => 'parquet'
);
