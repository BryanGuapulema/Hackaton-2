# etl_dim_employees.py
from athena_utils import run_athena, ym_from_run_month, BUCKET, DB

def run_dim_employees(run_month: str):
    y, m, m_z = ym_from_run_month(run_month)
    bronze_loc = f"s3://{BUCKET}/bronze/source=github/table=employee/run_month={y}-{m_z}/"
    silver_loc = f"s3://{BUCKET}/silver/dim=employee/"
    invalid_loc = f"s3://{BUCKET}/logs/invalid/dim=employee/run_month={y}-{m_z}/"

    sql = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DB}.bronze_employee_{y}_{int(m_z)} (
      EmployeeID        INT,
      ManagerID         INT,
      FirstName         STRING,
      LastName          STRING,
      FullName          STRING,
      JobTitle          STRING,
      OrganizationLevel INT,
      MaritalStatus     STRING,
      Gender            STRING,
      Territory         STRING,
      Country           STRING,
      "Group"           STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar'='\\\\')
    LOCATION '{bronze_loc}';

    WITH s AS (
      SELECT
        CAST(EmployeeID AS INT)        AS EmployeeID,
        TRY(CAST(ManagerID AS INT))    AS ManagerID,
        TRIM(FirstName)                AS FirstName,
        TRIM(LastName)                 AS LastName,
        TRIM(FullName)                 AS FullName,
        TRIM(JobTitle)                 AS JobTitle,
        TRY(CAST(OrganizationLevel AS INT)) AS OrganizationLevel,
        UPPER(TRIM(MaritalStatus))     AS MaritalStatus,
        UPPER(TRIM(Gender))            AS Gender,
        TRIM(Territory)                AS Territory,
        TRIM(Country)                  AS Country,
        TRIM("Group")                  AS "Group",
        ROW_NUMBER() OVER (PARTITION BY CAST(EmployeeID AS INT)
                           ORDER BY TRIM(FullName)) AS rn
      FROM {DB}.bronze_employee_{y}_{int(m_z)}
    ),
    s2 AS (
      SELECT *,
        -- Normaliza dominios
        CASE WHEN Gender IN ('M','MALE','H') THEN 'M'
             WHEN Gender IN ('F','FEMALE','Mujer') THEN 'F'
             ELSE 'U' END AS GenderNorm,
        CASE WHEN MaritalStatus IN ('S','SINGLE','SOLTERO','SOLTERA') THEN 'S'
             WHEN MaritalStatus IN ('M','MARRIED','CASADO','CASADA') THEN 'M'
             ELSE 'U' END AS MaritalNorm
      FROM s
    ),
    valid AS (
      SELECT * FROM s2
      WHERE EmployeeID IS NOT NULL AND rn=1 AND GenderNorm IN ('M','F') AND MaritalNorm IN ('S','M')
    ),
    invalid AS (
      SELECT s2.*,
        CASE
          WHEN EmployeeID IS NULL THEN 'PK_NULL'
          WHEN rn > 1 THEN 'DUPLICATE_PK'
          WHEN GenderNorm NOT IN ('M','F') THEN 'GENDER_INVALID'
          WHEN MaritalNorm NOT IN ('S','M') THEN 'MARITALSTATUS_INVALID'
          ELSE 'UNKNOWN' END AS REASON
      FROM s2
      WHERE NOT (EmployeeID IS NOT NULL AND rn=1 AND GenderNorm IN ('M','F') AND MaritalNorm IN ('S','M'))
    );

    CREATE TABLE {DB}.tmp_dim_employee_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='{silver_loc}') AS
    SELECT EmployeeID, ManagerID, FirstName, LastName, FullName, JobTitle,
           OrganizationLevel, MaritalNorm AS MaritalStatus, GenderNorm AS Gender,
           Territory, Country, "Group"
    FROM valid;

    CREATE TABLE {DB}.tmp_dim_employee_invalid_{y}_{int(m_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='{invalid_loc}') AS
    SELECT EmployeeID, ManagerID, FirstName, LastName, FullName, JobTitle,
           OrganizationLevel, MaritalStatus, Gender, Territory, Country, "Group", REASON
    FROM invalid;

    DROP TABLE IF EXISTS {DB}.tmp_dim_employee_{y}_{int(m_z)};
    DROP TABLE IF EXISTS {DB}.tmp_dim_employee_invalid_{y}_{int(m_z)};
    """
    run_athena(sql)
