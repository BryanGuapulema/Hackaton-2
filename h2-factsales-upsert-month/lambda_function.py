import os, re, urllib.parse, json, logging
from athena_utils import run_athena, get_scalar_int

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Ajustar si el bucket / prefijo cambia
BUCKET = os.environ.get("BUCKET", "bg-hack2-aw-datalake2")
ORDERS_PREFIX = "bronze/source=github/table=orders/"

# Regex para extraer run_month del nombre del archivo
RX_MONTH = re.compile(r"orders_(\d{4}-\d{2})\.csv$")

def parse_run_month_from_key(key: str):
    m = RX_MONTH.search(key)
    return m.group(1) if m else None

def make_s3_path(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"

def sql_count_month(run_month: str) -> str:
    return f"""
    SELECT COUNT(*) AS n
    FROM hack2_aw_catalog.fact_sales
    WHERE RunMonth = '{run_month}'
    """

def sql_count_file_rows(s3_path: str) -> str:
    return f"""
    SELECT COUNT(*) AS n
    FROM hack2_aw_catalog.ext_orders
    WHERE "$path" = '{s3_path}'
    """

def sql_insert_for(s3_path: str, run_month: str) -> str:
    # Una sola sentencia (INSERT). El DELETE se evita gracias al "skip si ya existe".
    return f"""
    INSERT INTO hack2_aw_catalog.fact_sales
    SELECT
      o.SalesOrderID            AS SalesOrderKey,
      o.SalesOrderDetailID      AS SalesOrderDetailKey,
      TRY(date_parse(o.OrderDate, '%m/%e/%Y')) AS OrderDate,
      TRY(date_parse(o.DueDate,   '%m/%e/%Y')) AS DueDate,
      TRY(date_parse(o.ShipDate,  '%m/%e/%Y')) AS ShipDate,
      o.EmployeeID              AS EmployeeKey,
      o.CustomerID              AS CustomerKey,
      o.ProductID               AS ProductKey,
      o.StoreID                 AS StoreKey,
      o.OrderQty,
      o.UnitPrice,
      o.UnitPriceDiscount,
      o.SubTotal,
      o.TaxAmt,
      o.Freight,
      o.TotalDue,
      o.LineTotal,
      '{run_month}'             AS RunMonth
    FROM hack2_aw_catalog.ext_orders o
    WHERE "$path" = '{s3_path}'
    """

def process_one_object(bucket: str, key: str):
    """
    Procesa un solo objeto S3 si y solo si:
      - Está bajo bronze/source...
      - Contiene table=orders/
      - Termina en .csv
      - Cumple patrón orders_YYYY-MM.csv
    """
    if bucket != BUCKET:
        return {"key": key, "status": "IGNORED_OTHER_BUCKET"}

    # 🔒 Filtro robusto por ruta
    if not key.startswith("bronze/source"):
        return {"key": key, "status": "IGNORED_PREFIX"}
    if "table=orders/" not in key:
        return {"key": key, "status": "IGNORED_NOT_ORDERS"}
    if not key.endswith(".csv"):
        return {"key": key, "status": "IGNORED_NOT_CSV"}

    run_month = parse_run_month_from_key(key)
    if not run_month:
        # Ej.: bronze/source=github/table=orders/some_file.csv → lo ignoramos
        return {"key": key, "status": "IGNORED_BAD_NAME"}

    s3_path = make_s3_path(bucket, key)
    logger.info(f"Processing {s3_path} run_month={run_month}")

    # 1) ¿ya existe ese mes en fact_sales?
    n_month = get_scalar_int(sql_count_month(run_month), default=0)
    if n_month > 0:
        logger.info(f"RunMonth {run_month} already present ({n_month} rows). Skipping.")
        return {"key": key, "run_month": run_month, "status": "SKIPPED_EXISTS", "rows_existing": n_month}

    # 2) ¿el archivo tiene filas?
    n_file = get_scalar_int(sql_count_file_rows(s3_path), default=0)
    if n_file == 0:
        logger.info(f"File has 0 rows. Skipping insert. path={s3_path}")
        return {"key": key, "run_month": run_month, "status": "SKIPPED_EMPTY_FILE", "rows_file": 0}

    # 3) INSERT solo ese archivo
    sql = sql_insert_for(s3_path, run_month)
    run_athena(sql)
    logger.info(f"Inserted month {run_month} from {s3_path} (file_rows_estimate={n_file})")

    # 4) Confirmación post-insert
    n_after = get_scalar_int(sql_count_month(run_month), default=0)
    return {"key": key, "run_month": run_month, "status": "SUCCEEDED", "rows_inserted": n_after}


def lambda_handler(event, context):
    """
    Soporta:
      - Evento S3 (Records[])
      - Invocación manual: {"run_month":"YYYY-MM"} (procesa el archivo con ese nombre)
      - Invocación manual: {"s3_key":"bronze/source=github/table=orders/orders_YYYY-MM.csv"}
    """
    results = []

    # 1) Evento S3 (lo más común)
    if "Records" in event:
        for rec in event.get("Records", []):
            bucket = rec["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
            try:
                results.append(process_one_object(bucket, key))
            except Exception as e:
                logger.exception(f"Error processing key={key}")
                results.append({"key": key, "status": "ERROR", "error": str(e)})
        return {"results": results}

    # 2) Invocación manual por run_month
    run_month = event.get("run_month")
    s3_key = event.get("s3_key")

    if s3_key:
        # Si te pasan el key exacto, úsalo directo
        try:
            results.append(process_one_object(BUCKET, s3_key))
        except Exception as e:
            logger.exception(f"Error processing key={s3_key}")
            results.append({"key": s3_key, "status": "ERROR", "error": str(e)})
        return {"results": results}

    if run_month:
        # Construye el key estándar para ese mes
        key = f"{ORDERS_PREFIX}orders_{run_month}.csv"
        try:
            results.append(process_one_object(BUCKET, key))
        except Exception as e:
            logger.exception(f"Error processing key={key}")
            results.append({"key": key, "status": "ERROR", "error": str(e)})
        return {"results": results}

    # Si no reconocemos el formato del evento
    return {"results": [], "status": "IGNORED_NO_INPUT"}
