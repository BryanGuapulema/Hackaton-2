# etl_dim_store.py
from athena_utils import run_athena, ym_from_run_month, BUCKET, DB

def run_dim_store(run_month: str):
    """SCD1: stores + storesBudget del snapshot de run_month → silver/dim=store/ (Parquet)."""
    y, m, m_z = ym_from_run_month(run_month)

    stores_loc = f"s3://{BUCKET}/bronze/source=mysql/table=stores/run_month={y}-{m_z}/"
    budget_loc = f"s3://{BUCKET}/bronze/source=excel/table=storesBudget/run_month={y}-{m_z}/"
    silver_loc = f"s3://{BUCKET}/silver/dim=store/"
    invalid_loc = f"s3://{BUCKET}/logs/invalid/dim=store/run_month={y}-{m_z}/"

    sql = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DB}.bronze_stores_{y}_{int(m_z)} (
      StoreID    INT,
      StoreName  STRING,
      EmployeeID INT
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar'='\\\\')
    LOCATION '{stores_loc}';

    CREATE EXTERNAL TABLE IF NOT EXISTS {DB}.bronze_storesbudget_{y}_{int(m_z)} (
      StoreID INT,
      Budget  STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar'='\\\\')
    LOCATION '{budget_loc}';

    WITH s AS (
      SELECT
        CAST(StoreID AS INT)         AS StoreID,
        TRIM(StoreName)              AS StoreName,
        TRY(CAST(EmployeeID AS INT)) AS EmployeeID,
        ROW_NUMBER() OVER (PARTITION BY CAST(StoreID AS INT) ORDER BY StoreName) AS rn
      FROM {DB}.bronze_stores_{y}_{int(m_z)}
    ),
    b AS (
      SELECT
        CAST(StoreID AS INT) AS StoreID,
        TRY(CAST(Budget AS DECIMAL(18,2))) AS Budget
      FROM {DB}.bronze_storesbudget_{y}_{int(m_z)}
    ),
    j AS (
      SELECT
        s.StoreID, s.StoreName, s.EmployeeID,
        COALESCE(b.Budget, CAST(0 AS DECIMAL(18,2))) AS Budget,
        s.rn,
        (s.StoreID IS NOT NULL)             AS pk_ok,
        (b.Budget IS NULL OR b.Budget >= 0) AS budget_ok
      FROM s
      LEFT JOIN b USING (StoreID)
    ),
    valid AS (SELECT * FROM j WHERE pk_ok AND rn=1 AND budget_ok),
    invalid AS (
      SELECT j.*,
        CASE
          WHEN NOT pk_ok   THEN 'PK_NULL'
          WHEN rn > 1      THEN 'DUPLICATE_PK'
          WHEN NOT budget_ok THEN 'BUDGET_INVALID'
          ELSE 'UNKNOWN'
        END AS REASON
      FROM j
      WHERE NOT (pk_ok AND rn=1 AND budget_ok)
    );

    CREATE TABLE {DB}.tmp_dim_store_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='{silver_loc}') AS
    SELECT StoreID, StoreName, EmployeeID, Budget
    FROM valid;

    CREATE TABLE {DB}.tmp_dim_store_invalid_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='{invalid_loc}') AS
    SELECT StoreID, StoreName, EmployeeID, Budget, REASON
    FROM invalid;

    DROP TABLE IF EXISTS {DB}.tmp_dim_store_{y}_{int(m_z)};
    DROP TABLE IF EXISTS {DB}.tmp_dim_store_invalid_{y}_{int(m_z)};
    """
    run_athena(sql)
