CREATE EXTERNAL TABLE IF NOT EXISTS hack2_aw_catalog.dim_customer (
  CustomerKey INT,
  FirstName   STRING,
  LastName    STRING,
  FullName    STRING
)
STORED AS PARQUET
LOCATION 's3://bg-hack2-aw-datalake2/warehouse/dim_customer/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
