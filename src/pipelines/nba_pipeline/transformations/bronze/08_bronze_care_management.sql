-- Bronze layer: Raw care management data ingestion
CREATE OR REFRESH STREAMING TABLE bronze_care_management
COMMENT 'Raw care management interactions data ingested from parquet files'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true',
  'contains_phi' = 'true'
)
AS SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw_data/care_management',
  format => 'parquet'
);
