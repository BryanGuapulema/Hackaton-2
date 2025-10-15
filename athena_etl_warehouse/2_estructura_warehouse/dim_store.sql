CREATE EXTERNAL TABLE IF NOT EXISTS hack2_aw_catalog.dim_store (
  StoreKey    INT,
  StoreName   STRING,
  EmployeeID  INT,
  Budget      DOUBLE
)
STORED AS PARQUET
LOCATION 's3://bg-hack2-aw-datalake2/warehouse/dim_store/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
