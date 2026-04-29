-- Bronze layer: Raw member data ingestion
CREATE OR REFRESH STREAMING TABLE bronze_members
COMMENT 'Raw member data ingested from parquet files'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true',
  'contains_pii' = 'true',
  'data_classification' = 'confidential'
)
AS SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw_data/members',
  format => 'parquet'
);
