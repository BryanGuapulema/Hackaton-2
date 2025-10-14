# etl_dim_customers.py
from athena_utils import run_athena, ym_from_run_month, BUCKET, DB

def run_dim_customers(run_month: str):
    y, m, m_z = ym_from_run_month(run_month)
    bronze_loc = f"s3://{BUCKET}/bronze/source=github/table=customers/run_month={y}-{m_z}/"
    silver_loc = f"s3://{BUCKET}/silver/dim=customer/"
    invalid_loc = f"s3://{BUCKET}/logs/invalid/dim=customer/run_month={y}-{m_z}/"

    # 1) Tabla externa Bronze
    run_athena(f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DB}.bronze_customers_{y}_{int(m_z)} (
      CustomerID INT,
      FirstName  STRING,
      LastName   STRING,
      FullName   STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar'='\\\\')
    LOCATION '{bronze_loc}';
    """)

    # 2) CTAS válidos → Silver
    run_athena(f"""
    CREATE TABLE {DB}.tmp_dim_customer_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='{silver_loc}')
    AS
    WITH s AS (
      SELECT CAST(CustomerID AS INT) AS CustomerID,
             TRIM(FirstName)         AS FirstName,
             TRIM(LastName)          AS LastName,
             TRIM(FullName)          AS FullName,
             ROW_NUMBER() OVER (PARTITION BY CAST(CustomerID AS INT)
                                ORDER BY TRIM(FullName)) AS rn
      FROM {DB}.bronze_customers_{y}_{int(m_z)}
    ),
    valid AS (SELECT * FROM s WHERE CustomerID IS NOT NULL AND rn=1)
    SELECT CustomerID, FirstName, LastName, FullName FROM valid;
    """)

    # 3) CTAS inválidos → logs/invalid
    run_athena(f"""
    CREATE TABLE {DB}.tmp_dim_customer_invalid_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY', external_location='{invalid_loc}')
    AS
    WITH s AS (
      SELECT CAST(CustomerID AS INT) AS CustomerID,
             TRIM(FirstName)         AS FirstName,
             TRIM(LastName)          AS LastName,
             TRIM(FullName)          AS FullName,
             ROW_NUMBER() OVER (PARTITION BY CAST(CustomerID AS INT)
                                ORDER BY TRIM(FullName)) AS rn
      FROM {DB}.bronze_customers_{y}_{int(m_z)}
    ),
    invalid AS (
      SELECT s.*,
        CASE WHEN CustomerID IS NULL THEN 'PK_NULL'
             WHEN rn > 1 THEN 'DUPLICATE_PK'
             ELSE 'UNKNOWN' END AS REASON
      FROM s
      WHERE NOT (CustomerID IS NOT NULL AND rn=1)
    )
    SELECT CustomerID, FirstName, LastName, FullName, REASON FROM invalid;
    """)

    # 4) Drops
    run_athena(f"DROP TABLE IF EXISTS {DB}.tmp_dim_customer_{y}_{int(m_z)};")
    run_athena(f"DROP TABLE IF EXISTS {DB}.tmp_dim_customer_invalid_{y}_{int(m_z)};")
