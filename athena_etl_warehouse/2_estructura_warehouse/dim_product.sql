CREATE EXTERNAL TABLE IF NOT EXISTS hack2_aw_catalog.dim_product (
  ProductKey      INT,
  ProductNumber   STRING,
  ProductName     STRING,
  ModelName       STRING,
  MakeFlag        INT,
  StandardCost    DOUBLE,
  ListPrice       DOUBLE,
  SubCategoryID   INT,
  SubCategoryName STRING,
  CategoryID      INT,
  CategoryName    STRING
)
STORED AS PARQUET
LOCATION 's3://bg-hack2-aw-datalake2/warehouse/dim_product/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
