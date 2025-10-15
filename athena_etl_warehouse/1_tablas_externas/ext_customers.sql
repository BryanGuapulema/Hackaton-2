CREATE EXTERNAL TABLE IF NOT EXISTS hack2_aw_catalog.ext_customers (
  CustomerID        INT,
  FirstName         STRING,
  LastName          STRING,
  FullName          STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar'='\"', 'escapeChar'='\\')
LOCATION 's3://bg-hack2-aw-datalake2/bronze/source=github/table=customers/'
TBLPROPERTIES ('skip.header.line.count'='1');

SELECT * FROM ext_customers LIMIT 5;