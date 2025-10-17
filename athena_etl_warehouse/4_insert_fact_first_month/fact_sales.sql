INSERT INTO hack2_aw_catalog.fact_sales
SELECT
  o.SalesOrderID            AS SalesOrderKey,
  o.SalesOrderDetailID      AS SalesOrderDetailKey,

  TRY(date_parse(o.OrderDate, '%m/%e/%Y')) AS OrderDate,
  TRY(date_parse(o.DueDate,   '%m/%e/%Y')) AS DueDate,
  TRY(date_parse(o.ShipDate,  '%m/%e/%Y')) AS ShipDate,

  o.EmployeeID              AS EmployeeKey,
  o.CustomerID              AS CustomerKey,
  o.ProductID               AS ProductKey,
  o.StoreID                 AS StoreKey,

  o.OrderQty,
  o.UnitPrice,
  o.UnitPriceDiscount,
  o.SubTotal,
  o.TaxAmt,
  o.Freight,
  o.TotalDue,
  o.LineTotal,

  '2011-05'                 AS RunMonth
FROM hack2_aw_catalog.ext_orders o
WHERE "$path" = 's3://bg-hack2-aw-datalake2/bronze/source=github/table=orders/orders_2011-05.csv';
