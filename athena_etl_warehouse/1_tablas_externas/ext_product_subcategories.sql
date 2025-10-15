CREATE EXTERNAL TABLE IF NOT EXISTS hack2_aw_catalog.ext_product_subcategories (
  SubCategoryID   INT,
  CategoryID      INT,
  SubCategoryName STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"','escapeChar'='\\')
LOCATION 's3://bg-hack2-aw-datalake2/bronze/source=github/table=productSubcategories/'
TBLPROPERTIES ('skip.header.line.count'='1');