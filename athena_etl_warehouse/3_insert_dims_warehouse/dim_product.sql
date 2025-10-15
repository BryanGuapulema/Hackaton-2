INSERT INTO hack2_aw_catalog.dim_product
SELECT
  p.ProductID                   AS ProductKey,
  TRIM(p.ProductNumber)         AS ProductNumber,
  TRIM(p.ProductName)           AS ProductName,
  TRIM(p.ModelName)             AS ModelName,
  TRY(CAST(p.MakeFlag AS INT))  AS MakeFlag,
  p.StandardCost,
  p.ListPrice,
  p.SubCategoryID,
  TRIM(ps.SubCategoryName)      AS SubCategoryName,
  ps.CategoryID,
  TRIM(pc.CategoryName)         AS CategoryName
FROM hack2_aw_catalog.ext_products p
LEFT JOIN hack2_aw_catalog.ext_product_subcategories ps
  ON p.SubCategoryID = ps.SubCategoryID
LEFT JOIN hack2_aw_catalog.ext_product_categories pc
  ON ps.CategoryID = pc.CategoryID
WHERE p.ProductID IS NOT NULL;