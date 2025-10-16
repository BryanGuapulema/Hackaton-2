CREATE OR REPLACE VIEW hack2_aw_catalog.v_sales AS
SELECT
  f.SalesOrderKey,
  f.SalesOrderDetailKey,
  CAST(f.OrderDate AS DATE) AS OrderDate,
  f.RunMonth,
  c.FullName AS CustomerName,
  e.FullName AS EmployeeName, e.JobTitle, e.Territory, e.Country,
  f.StoreKey,                               -- usa la FK directa del hecho
  f.ProductKey,                             -- idem
  p.ProductName, p.ModelName,
  p.SubCategoryName, p.CategoryName,
  f.OrderQty, f.UnitPrice, f.UnitPriceDiscount, f.SubTotal, f.TaxAmt, f.Freight, f.TotalDue, f.LineTotal
FROM hack2_aw_catalog.fact_sales f
LEFT JOIN hack2_aw_catalog.dim_customer c ON f.CustomerKey = c.CustomerKey
LEFT JOIN hack2_aw_catalog.dim_employee e ON f.EmployeeKey = e.EmployeeKey
LEFT JOIN hack2_aw_catalog.dim_store    s ON f.StoreKey    = s.StoreKey
LEFT JOIN hack2_aw_catalog.dim_product  p ON f.ProductKey  = p.ProductKey;
