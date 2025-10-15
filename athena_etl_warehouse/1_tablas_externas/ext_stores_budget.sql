CREATE EXTERNAL TABLE IF NOT EXISTS hack2_aw_catalog.ext_stores_budget (
  StoreID  INT,
  Budget   DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"','escapeChar'='\\')
LOCATION 's3://bg-hack2-aw-datalake2/bronze/source=excel/table=storesBudget/csv/'

TBLPROPERTIES ('skip.header.line.count'='1');

SELECT * FROM ext_stores_budget LIMIT 5;