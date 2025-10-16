CREATE OR REPLACE VIEW hack2_aw_catalog.v_dim_store AS
SELECT
  s.StoreKey,
  s.StoreName,
  s.Budget
FROM hack2_aw_catalog.dim_store s;
