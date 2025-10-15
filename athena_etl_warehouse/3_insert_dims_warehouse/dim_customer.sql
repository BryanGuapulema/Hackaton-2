INSERT INTO hack2_aw_catalog.dim_customer
SELECT
  TRY(CAST(CustomerID AS INT)) AS CustomerKey,
  TRIM(FirstName)              AS FirstName,
  TRIM(LastName)               AS LastName,
  TRIM(FullName)               AS FullName
FROM hack2_aw_catalog.ext_customers
WHERE TRY(CAST(CustomerID AS INT)) IS NOT NULL;
