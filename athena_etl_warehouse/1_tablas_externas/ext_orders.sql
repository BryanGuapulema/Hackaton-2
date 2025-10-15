CREATE EXTERNAL TABLE IF NOT EXISTS hack2_aw_catalog.ext_orders (
  SalesOrderID        INT,
  SalesOrderDetailID  INT,
  OrderDate           STRING,
  DueDate             STRING,
  ShipDate            STRING,
  EmployeeID          INT,
  CustomerID          INT,
  SubTotal            DOUBLE,
  TaxAmt              DOUBLE,
  Freight             DOUBLE,
  TotalDue            DOUBLE,
  ProductID           INT,
  OrderQty            INT,
  UnitPrice           DOUBLE,
  UnitPriceDiscount   DOUBLE,
  LineTotal           DOUBLE,
  StoreID             INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar'=',','quoteChar'='\"','escapeChar'='\\')
LOCATION 's3://bg-hack2-aw-datalake2/bronze/source=github/table=orders/'
TBLPROPERTIES ('skip.header.line.count'='1');


SELECT * FROM ext_orders LIMIT 5;