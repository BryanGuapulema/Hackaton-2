# etl_dim_products.py
from athena_utils import run_athena, ym_from_run_month, BUCKET, DB

def run_dim_products(run_month: str):
    """Crea Category, SubCategory y Product en Silver; valida PKs y dedup (FKs básicas por no-nulos)."""
    y, m, m_z = ym_from_run_month(run_month)

    cat_loc  = f"s3://{BUCKET}/bronze/source=github/table=productCategories/run_month={y}-{m_z}/"
    sub_loc  = f"s3://{BUCKET}/bronze/source=github/table=productSubcategories/run_month={y}-{m_z}/"
    prod_loc = f"s3://{BUCKET}/bronze/source=github/table=products/run_month={y}-{m_z}/"

    silver_cat  = f"s3://{BUCKET}/silver/dim=product/Category/"
    silver_sub  = f"s3://{BUCKET}/silver/dim=product/SubCategory/"
    silver_prod = f"s3://{BUCKET}/silver/dim=product/Product/"

    invalid_cat  = f"s3://{BUCKET}/logs/invalid/dim=productCategory/run_month={y}-{m_z}/"
    invalid_sub  = f"s3://{BUCKET}/logs/invalid/dim=productSubcategory/run_month={y}-{m_z}/"
    invalid_prod = f"s3://{BUCKET}/logs/invalid/dim=product/run_month={y}-{m_z}/"

    # 1) Tablas externas Bronze
    run_athena(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DB}.bronze_productcategories_{y}_{int(m_z)} (
      CategoryID   INT,
      CategoryName STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar'='\\\\')
    LOCATION '{cat_loc}';
    """)
    run_athena(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DB}.bronze_productsubcategories_{y}_{int(m_z)} (
      SubCategoryID INT,
      CategoryID    INT,
      SubCategoryName STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar'='\\\\')
    LOCATION '{sub_loc}';
    """)
    run_athena(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DB}.bronze_products_{y}_{int(m_z)} (
      ProductID     INT,
      ProductNumber STRING,
      ProductName   STRING,
      ModelName     STRING,
      MakeFlag      STRING,
      StandardCost  DOUBLE,
      ListPrice     DOUBLE,
      SubCategoryID INT
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar'='\\\\')
    LOCATION '{prod_loc}';
    """)

    # 2) CATEGORY — válidos
    run_athena(f"""
    CREATE TABLE {DB}.tmp_dim_product_category_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='{silver_cat}')
    AS
    WITH c AS (
      SELECT CAST(CategoryID AS INT) AS CategoryID,
             TRIM(CategoryName)      AS CategoryName,
             ROW_NUMBER() OVER (PARTITION BY CAST(CategoryID AS INT)
                                ORDER BY TRIM(CategoryName)) AS rn
      FROM {DB}.bronze_productcategories_{y}_{int(m_z)}
    ),
    c_valid AS (SELECT * FROM c WHERE CategoryID IS NOT NULL AND rn=1)
    SELECT CategoryID, CategoryName FROM c_valid;
    """)
    # 3) CATEGORY — inválidos
    run_athena(f"""
    CREATE TABLE {DB}.tmp_dim_product_category_invalid_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='{invalid_cat}')
    AS
    WITH c AS (
      SELECT CAST(CategoryID AS INT) AS CategoryID,
             TRIM(CategoryName)      AS CategoryName,
             ROW_NUMBER() OVER (PARTITION BY CAST(CategoryID AS INT)
                                ORDER BY TRIM(CategoryName)) AS rn
      FROM {DB}.bronze_productcategories_{y}_{int(m_z)}
    ),
    c_invalid AS (
      SELECT *, CASE WHEN CategoryID IS NULL THEN 'PK_NULL'
                     WHEN rn>1 THEN 'DUPLICATE_PK'
                     ELSE 'UNKNOWN' END AS REASON
      FROM c
      WHERE NOT (CategoryID IS NOT NULL AND rn=1)
    )
    SELECT CategoryID, CategoryName, REASON FROM c_invalid;
    """)
    run_athena(f"DROP TABLE IF EXISTS {DB}.tmp_dim_product_category_{y}_{int(m_z)};")
    run_athena(f"DROP TABLE IF EXISTS {DB}.tmp_dim_product_category_invalid_{y}_{int(m_z)};")

    # 4) SUBCATEGORY — válidos
    run_athena(f"""
    CREATE TABLE {DB}.tmp_dim_product_subcategory_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='{silver_sub}')
    AS
    WITH s AS (
      SELECT CAST(SubCategoryID AS INT) AS SubCategoryID,
             CAST(CategoryID AS INT)    AS CategoryID,
             TRIM(SubCategoryName)      AS SubCategoryName,
             ROW_NUMBER() OVER (PARTITION BY CAST(SubCategoryID AS INT)
                                ORDER BY TRIM(SubCategoryName)) AS rn
      FROM {DB}.bronze_productsubcategories_{y}_{int(m_z)}
    ),
    s_valid AS (
      SELECT * FROM s
      WHERE SubCategoryID IS NOT NULL AND rn=1 AND CategoryID IS NOT NULL
    )
    SELECT SubCategoryID, CategoryID, SubCategoryName FROM s_valid;
    """)
    # 5) SUBCATEGORY — inválidos
    run_athena(f"""
    CREATE TABLE {DB}.tmp_dim_product_subcategory_invalid_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='{invalid_sub}')
    AS
    WITH s AS (
      SELECT CAST(SubCategoryID AS INT) AS SubCategoryID,
             CAST(CategoryID AS INT)    AS CategoryID,
             TRIM(SubCategoryName)      AS SubCategoryName,
             ROW_NUMBER() OVER (PARTITION BY CAST(SubCategoryID AS INT)
                                ORDER BY TRIM(SubCategoryName)) AS rn
      FROM {DB}.bronze_productsubcategories_{y}_{int(m_z)}
    ),
    s_invalid AS (
      SELECT s.*,
        CASE
          WHEN s.SubCategoryID IS NULL THEN 'PK_NULL'
          WHEN s.rn > 1 THEN 'DUPLICATE_PK'
          WHEN s.CategoryID IS NULL THEN 'CAT_FK_NULL'
          ELSE 'UNKNOWN'
        END AS REASON
      FROM s
      WHERE NOT (s.SubCategoryID IS NOT NULL AND s.rn=1 AND s.CategoryID IS NOT NULL)
    )
    SELECT SubCategoryID, CategoryID, SubCategoryName, REASON FROM s_invalid;
    """)
    run_athena(f"DROP TABLE IF EXISTS {DB}.tmp_dim_product_subcategory_{y}_{int(m_z)};")
    run_athena(f"DROP TABLE IF EXISTS {DB}.tmp_dim_product_subcategory_invalid_{y}_{int(m_z)};")

    # 6) PRODUCT — válidos
    run_athena(f"""
    CREATE TABLE {DB}.tmp_dim_product_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='{silver_prod}')
    AS
    WITH p AS (
      SELECT CAST(ProductID AS INT)     AS ProductID,
             TRIM(ProductNumber)        AS ProductNumber,
             TRIM(ProductName)          AS ProductName,
             NULLIF(TRIM(ModelName),'') AS ModelName,
             CASE UPPER(TRIM(MakeFlag))
               WHEN '1' THEN TRUE WHEN 'TRUE' THEN TRUE WHEN 'Y' THEN TRUE
               ELSE FALSE END           AS MakeFlag,
             TRY(CAST(StandardCost AS DOUBLE)) AS StandardCost,
             TRY(CAST(ListPrice AS DOUBLE))    AS ListPrice,
             CAST(SubCategoryID AS INT) AS SubCategoryID,
             ROW_NUMBER() OVER (PARTITION BY CAST(ProductID AS INT)
                                ORDER BY TRIM(ProductName)) AS rn
      FROM {DB}.bronze_products_{y}_{int(m_z)}
    ),
    p_valid AS (
      SELECT * FROM p
      WHERE ProductID IS NOT NULL AND rn=1 AND SubCategoryID IS NOT NULL
    )
    SELECT ProductID, ProductNumber, ProductName, ModelName, MakeFlag,
           StandardCost, ListPrice, SubCategoryID
    FROM p_valid;
    """)
    # 7) PRODUCT — inválidos
    run_athena(f"""
    CREATE TABLE {DB}.tmp_dim_product_invalid_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='{invalid_prod}')
    AS
    WITH p AS (
      SELECT CAST(ProductID AS INT)     AS ProductID,
             TRIM(ProductNumber)        AS ProductNumber,
             TRIM(ProductName)          AS ProductName,
             NULLIF(TRIM(ModelName),'') AS ModelName,
             CASE UPPER(TRIM(MakeFlag))
               WHEN '1' THEN TRUE WHEN 'TRUE' THEN TRUE WHEN 'Y' THEN TRUE
               ELSE FALSE END           AS MakeFlag,
             TRY(CAST(StandardCost AS DOUBLE)) AS StandardCost,
             TRY(CAST(ListPrice AS DOUBLE))    AS ListPrice,
             CAST(SubCategoryID AS INT) AS SubCategoryID,
             ROW_NUMBER() OVER (PARTITION BY CAST(ProductID AS INT)
                                ORDER BY TRIM(ProductName)) AS rn
      FROM {DB}.bronze_products_{y}_{int(m_z)}
    ),
    p_invalid AS (
      SELECT p.*,
        CASE
          WHEN p.ProductID IS NULL THEN 'PK_NULL'
          WHEN p.rn > 1 THEN 'DUPLICATE_PK'
          WHEN p.SubCategoryID IS NULL THEN 'SUBCAT_FK_NULL'
          ELSE 'UNKNOWN'
        END AS REASON
      FROM p
      WHERE NOT (p.ProductID IS NOT NULL AND p.rn=1 AND p.SubCategoryID IS NOT NULL)
    )
    SELECT ProductID, ProductNumber, ProductName, ModelName, MakeFlag,
           StandardCost, ListPrice, SubCategoryID, REASON
    FROM p_invalid;
    """)
    run_athena(f"DROP TABLE IF EXISTS {DB}.tmp_dim_product_{y}_{int(m_z)};")
    run_athena(f"DROP TABLE IF EXISTS {DB}.tmp_dim_product_invalid_{y}_{int(m_z)};")
