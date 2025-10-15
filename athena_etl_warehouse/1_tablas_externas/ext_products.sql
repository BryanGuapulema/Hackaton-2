CREATE EXTERNAL TABLE hack2_aw_catalog.ext_products (
  ProductID        INT,
  ProductNumber    STRING,
  ProductName      STRING,
  ModelName        STRING,
  MakeFlag         INT,
  StandardCost     DOUBLE,
  ListPrice        DOUBLE,
  SubCategoryID    INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar'     = '\"',
  'escapeChar'    = '\\'
)
LOCATION 's3://bg-hack2-aw-datalake2/bronze/source=github/table=products/'
TBLPROPERTIES ('skip.header.line.count'='1');


SELECT * FROM ext_products LIMIT 5;