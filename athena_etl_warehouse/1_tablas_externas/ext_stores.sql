CREATE EXTERNAL TABLE IF NOT EXISTS hack2_aw_catalog.ext_stores (
  StoreID     INT,
  StoreName   STRING,
  EmployeeID  INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"','escapeChar'='\\')
LOCATION 's3://bg-hack2-aw-datalake2/bronze/source=mysql/table=stores/'
TBLPROPERTIES ('skip.header.line.count'='1');

SELECT * FROM ext_stores LIMIT 5;