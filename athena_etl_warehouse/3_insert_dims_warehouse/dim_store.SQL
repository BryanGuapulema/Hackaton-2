INSERT INTO hack2_aw_catalog.dim_store
SELECT
  s.StoreID         AS StoreKey,
  TRIM(s.StoreName) AS StoreName,
  s.EmployeeID      AS EmployeeID,
  b.Budget          AS Budget
FROM hack2_aw_catalog.ext_stores s
LEFT JOIN hack2_aw_catalog.ext_stores_budget b
  ON s.StoreID = b.StoreID
WHERE s.StoreID IS NOT NULL;
