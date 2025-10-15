CREATE EXTERNAL TABLE IF NOT EXISTS hack2_aw_catalog.fact_sales (
  SalesOrderKey        INT,
  SalesOrderDetailKey  INT,
  OrderDate            DATE,
  DueDate              DATE,
  ShipDate             DATE,
  EmployeeKey          INT,
  CustomerKey          INT,
  ProductKey           INT,
  StoreKey             INT,
  OrderQty             INT,
  UnitPrice            DOUBLE,
  UnitPriceDiscount    DOUBLE,
  SubTotal             DOUBLE,
  TaxAmt               DOUBLE,
  Freight              DOUBLE,
  TotalDue             DOUBLE,
  LineTotal            DOUBLE,
  RunMonth             STRING
)
STORED AS PARQUET
LOCATION 's3://bg-hack2-aw-datalake2/warehouse/fact_sales/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
