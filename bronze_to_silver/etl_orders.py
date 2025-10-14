# etl_orders.py
from athena_utils import run_athena, ym_from_run_month, BUCKET, DB

ORDERS_BRONZE_LOC = f"s3://{BUCKET}/bronze/source=github/table=orders/"

def _ensure_bronze_orders(year: int, month: int, month_z: str):
    create_tbl = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {DB}.bronze_orders (
      SalesOrderID           INT,
      SalesOrderDetailID     INT,
      OrderDate              STRING,
      DueDate                STRING,
      ShipDate               STRING,
      EmployeeID             INT,
      CustomerID             INT,
      SubTotal               DOUBLE,
      TaxAmt                 DOUBLE,
      Freight                DOUBLE,
      TotalDue               DOUBLE,
      ProductID              INT,
      OrderQty               INT,
      UnitPrice              DOUBLE,
      UnitPriceDiscount      DOUBLE,
      LineTotal              DOUBLE,
      StoreID                INT
    )
    PARTITIONED BY (year INT, month INT)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar' = ',', 'quoteChar' = '"', 'escapeChar'='\\\\')
    LOCATION '{ORDERS_BRONZE_LOC}';
    """
    run_athena(create_tbl)

    add_part = f"""
    ALTER TABLE {DB}.bronze_orders
    ADD IF NOT EXISTS PARTITION (year={year}, month={int(month_z)})
    LOCATION '{ORDERS_BRONZE_LOC}year={year}/month={month_z}/';
    """
    run_athena(add_part)

def _ctas_orders_to_silver(year: int, month: int, month_z: str):
    dest_valid   = f"s3://{BUCKET}/silver/domain=sales/year={year}/month={month_z}/"
    dest_invalid = f"s3://{BUCKET}/logs/invalid/orders/year={year}/month={month_z}/"

    # 1) CTAS VÁLIDOS (una sola sentencia)
    sql_valid = f"""
    WITH stage AS (
      SELECT
        CAST(SalesOrderID AS INT)              AS SalesOrderID,
        CAST(SalesOrderDetailID AS INT)        AS SalesOrderDetailID,
        CAST(date_parse(OrderDate, '%m/%d/%Y') AS DATE)  AS OrderDate,
        CAST(EmployeeID AS INT)                AS EmployeeID,
        CAST(CustomerID AS INT)                AS CustomerID,
        CAST(ProductID AS INT)                 AS ProductID,
        CAST(StoreID AS INT)                   AS StoreID,
        CAST(OrderQty AS INT)                  AS OrderQty,
        CAST(UnitPrice AS DOUBLE)              AS UnitPrice,
        CAST(UnitPriceDiscount AS DOUBLE)      AS UnitPriceDiscount,
        CAST(LineTotal AS DOUBLE)              AS LineTotal,
        CAST(SubTotal AS DOUBLE)               AS SubTotal,
        CAST(TaxAmt AS DOUBLE)                 AS TaxAmt,
        CAST(Freight AS DOUBLE)                AS Freight,
        CAST(TotalDue AS DOUBLE)               AS TotalDue,
        ROW_NUMBER() OVER (
          PARTITION BY SalesOrderID, SalesOrderDetailID
          ORDER BY TRY(date_parse(OrderDate, '%m/%d/%Y')) DESC
        ) AS rn
      FROM {DB}.bronze_orders
      WHERE year={year} AND month={int(month_z)}
    ),
    checks AS (
      SELECT
        s.*,
        (SalesOrderID IS NOT NULL AND SalesOrderDetailID IS NOT NULL)               AS pk_ok,
        (OrderDate IS NOT NULL)                                                     AS date_ok,
        (OrderQty > 0)                                                              AS qty_ok,
        (UnitPrice >= 0)                                                            AS price_ok,
        (UnitPriceDiscount >= 0 AND UnitPriceDiscount <= 1)                         AS disc_ok,
        (SubTotal >= 0 AND TaxAmt >= 0 AND Freight >= 0 AND TotalDue >= 0
          AND LineTotal >= 0)                                                       AS nonneg_ok,
        (abs(LineTotal - (OrderQty * UnitPrice * (1 - UnitPriceDiscount))) <= 0.01) AS line_ok,
        (abs(TotalDue - (SubTotal + TaxAmt + Freight)) <= 0.01)                     AS total_ok,
        (rn = 1)                                                                    AS dedup_ok
      FROM stage s
    ),
    valid AS (
      SELECT * FROM checks
      WHERE pk_ok AND date_ok AND qty_ok AND price_ok AND disc_ok
        AND nonneg_ok AND line_ok AND total_ok AND dedup_ok
    )
    CREATE TABLE {DB}.tmp_orders_valid_{year}_{int(month_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='{dest_valid}') AS
    SELECT
      SalesOrderID, SalesOrderDetailID, OrderDate,
      EmployeeID, CustomerID, ProductID, StoreID,
      OrderQty, UnitPrice, UnitPriceDiscount, LineTotal,
      SubTotal, TaxAmt, Freight, TotalDue
    FROM valid
    ;
    """
    run_athena(sql_valid)

    # 2) CTAS INVÁLIDOS (otra sentencia separada)
    sql_invalid = f"""
    WITH stage AS (
      SELECT
        CAST(SalesOrderID AS INT)              AS SalesOrderID,
        CAST(SalesOrderDetailID AS INT)        AS SalesOrderDetailID,
        CAST(date_parse(OrderDate, '%m/%d/%Y') AS DATE)  AS OrderDate,
        CAST(EmployeeID AS INT)                AS EmployeeID,
        CAST(CustomerID AS INT)                AS CustomerID,
        CAST(ProductID AS INT)                 AS ProductID,
        CAST(StoreID AS INT)                   AS StoreID,
        CAST(OrderQty AS INT)                  AS OrderQty,
        CAST(UnitPrice AS DOUBLE)              AS UnitPrice,
        CAST(UnitPriceDiscount AS DOUBLE)      AS UnitPriceDiscount,
        CAST(LineTotal AS DOUBLE)              AS LineTotal,
        CAST(SubTotal AS DOUBLE)               AS SubTotal,
        CAST(TaxAmt AS DOUBLE)                 AS TaxAmt,
        CAST(Freight AS DOUBLE)                AS Freight,
        CAST(TotalDue AS DOUBLE)               AS TotalDue,
        ROW_NUMBER() OVER (
          PARTITION BY SalesOrderID, SalesOrderDetailID
          ORDER BY TRY(date_parse(OrderDate, '%m/%d/%Y')) DESC
        ) AS rn
      FROM {DB}.bronze_orders
      WHERE year={year} AND month={int(month_z)}
    ),
    checks AS (
      SELECT
        s.*,
        (SalesOrderID IS NOT NULL AND SalesOrderDetailID IS NOT NULL)               AS pk_ok,
        (OrderDate IS NOT NULL)                                                     AS date_ok,
        (OrderQty > 0)                                                              AS qty_ok,
        (UnitPrice >= 0)                                                            AS price_ok,
        (UnitPriceDiscount >= 0 AND UnitPriceDiscount <= 1)                         AS disc_ok,
        (SubTotal >= 0 AND TaxAmt >= 0 AND Freight >= 0 AND TotalDue >= 0
          AND LineTotal >= 0)                                                       AS nonneg_ok,
        (abs(LineTotal - (OrderQty * UnitPrice * (1 - UnitPriceDiscount))) <= 0.01) AS line_ok,
        (abs(TotalDue - (SubTotal + TaxAmt + Freight)) <= 0.01)                     AS total_ok,
        (rn = 1)                                                                    AS dedup_ok
      FROM stage s
    ),
    invalid AS (
      SELECT
        c.*,
        CASE
          WHEN NOT pk_ok    THEN 'PK_NULL'
          WHEN NOT date_ok  THEN 'BAD_ORDERDATE'
          WHEN NOT dedup_ok THEN 'DUPLICATE_PK'
          WHEN NOT qty_ok   THEN 'QTY_LE_0'
          WHEN NOT price_ok THEN 'PRICE_LT_0'
          WHEN NOT disc_ok  THEN 'DISCOUNT_OUT_OF_RANGE'
          WHEN NOT nonneg_ok THEN 'NEGATIVE_AMOUNTS'
          WHEN NOT line_ok  THEN 'LINE_MISMATCH'
          WHEN NOT total_ok THEN 'TOTAL_MISMATCH'
          ELSE 'UNKNOWN'
        END AS REASON
      FROM checks c
      WHERE NOT (
        pk_ok AND date_ok AND qty_ok AND price_ok AND disc_ok
        AND nonneg_ok AND line_ok AND total_ok AND dedup_ok
      )
    )
    CREATE TABLE {DB}.tmp_orders_invalid_{year}_{int(month_z)}
    WITH (format='PARQUET', parquet_compression='SNAPPY',
          external_location='{dest_invalid}') AS
    SELECT
      SalesOrderID, SalesOrderDetailID, OrderDate,
      EmployeeID, CustomerID, ProductID, StoreID,
      OrderQty, UnitPrice, UnitPriceDiscount, LineTotal,
      SubTotal, TaxAmt, Freight, TotalDue, REASON
    FROM invalid
    ;
    """
    run_athena(sql_invalid)

    # 3) DROP tmp válidos
    run_athena(f"DROP TABLE IF EXISTS {DB}.tmp_orders_valid_{year}_{int(month_z)};")

    # 4) DROP tmp inválidos
    run_athena(f"DROP TABLE IF EXISTS {DB}.tmp_orders_invalid_{year}_{int(month_z)};")


def run_orders(run_month: str):
    y, m, m_z = ym_from_run_month(run_month)
    _ensure_bronze_orders(y, m, m_z)
    _ctas_orders_to_silver(y, m, m_z)
