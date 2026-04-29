-- Bronze layer: Raw claims data ingestion
CREATE OR REFRESH STREAMING TABLE bronze_claims
COMMENT 'Raw claims data ingested from parquet files'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true',
  'contains_phi' = 'true',
  'data_classification' = 'highly_confidential'
)
AS SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw_data/claims',
  format => 'parquet'
);
