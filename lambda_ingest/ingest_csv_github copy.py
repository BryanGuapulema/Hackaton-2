import os, json, csv, io, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

# ---------- Clients ----------
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# ---------- Env ----------
BUCKET = os.environ['BUCKET_NAME']
CONTROL_TABLE = os.environ['CONTROL_TABLE']
ERROR_TABLE = os.environ['ERROR_TABLE']

# ---------- Utils ----------
def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _ts():
    return int(datetime.now(timezone.utc).timestamp())

def _put_control(item):
    try:
        dynamodb.Table(CONTROL_TABLE).put_item(Item=item)
    except Exception as e:
        print("CONTROL LOG ERROR:", e)

def _put_error(item):
    try:
        dynamodb.Table(ERROR_TABLE).put_item(Item=item)
    except Exception as e:
        print("ERROR LOG ERROR:", e)

def _object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if code == 404:
            return False
        raise

def _upload_bytes(key, data: bytes):
    s3.put_object(Bucket=BUCKET, Key=key, Body=data)

def _clear_prefix(bucket, prefix):
    # borra objetos bajo el prefijo (para overwrite seguro)
    s3r = boto3.resource('s3')
    b = s3r.Bucket(bucket)
    b.objects.filter(Prefix=prefix).delete()

def _load_manifest():
    """
    Espera un manifest en:
      s3://{BUCKET}/bronze/source_metadata/sources.json
    con entradas tipo:
    {
      "sources": [
        {
          "type": "csv_github",
          "table": "orders",
          "url_or_query": "https://raw.githubusercontent.com/.../orders.csv",
          "target_bronze_prefix": "bronze/source=github/table=orders/",
          "date_field": "OrderDate",
          "date_format": "MM-dd-yyyy"
        },
        {
          "type": "csv_github",
          "table": "customers",
          "url_or_query": "...",
          "target_bronze_prefix": "bronze/source=github/table=customers/"
        },
        ...
      ],
      "run_defaults": {}
    }
    """
    key = "bronze/source_metadata/sources.json"
    body = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
    m = json.loads(body.decode('utf-8'))
    return m.get('sources', []), m.get('run_defaults', {})

def _month_parts(run_month: str):
    y, m = run_month.split("-")
    return int(y), int(m)

def _match_run_month(date_str, pattern, run_month):
    """
    Devuelve True si date_str cae dentro del run_month (YYYY-MM).
    Admite varios formatos de fecha.
    """
    if not date_str:
        return False
    date_str = date_str.strip().split("T")[0].split(" ")[0]

    fmts = []
    # patrón declarado en manifest
    if pattern in ("M/d/yyyy", "MM/dd/yyyy"):
        fmts.append("%m/%d/%Y")
    elif pattern in ("MM-dd-yyyy", "M-d-yyyy"):
        fmts.append("%m-%d-%Y")

    # alternativas comunes
    fmts += ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y", "%m-%d-%Y"]

    Y, M = _month_parts(run_month)
    for f in fmts:
        try:
            dt = datetime.strptime(date_str, f)
            return dt.year == Y and dt.month == M
        except Exception:
            continue
    return False

def _log_table_result(run_id, run_month, table, status, records_out, note=None, source="github"):
    item = {
        "run_id": f"{run_id}#{table}",
        "run_month": run_month,
        "source": source,
        "table": table,
        "status": status,
        "records_out": int(records_out),
        "started_at": _now_iso(),
        "ended_at": _now_iso()
    }
    if note:
        item["note"] = note
    _put_control(item)

# ---------- Core ----------
def _process_orders_month(src, run_month, allow_overwrite, run_id):
    """
    Procesa un solo mes para 'orders'.
    - Filtra por fecha (run_month) desde el CSV de origen.
    - Escribe bronze/orders_YYYY-MM.csv (respeta allow_overwrite).
    - Loguea resultado en CONTROL_TABLE.
    """
    table = src['table']                      # "orders"
    url = src['url_or_query']
    target_prefix = src['target_bronze_prefix']  # e.g. "bronze/source=github/table=orders/"
    source_type = src.get("source", "github")

    # Descarga CSV fuente
    with urllib.request.urlopen(url) as resp:
        raw = resp.read().decode('utf-8', errors='replace')
    reader = csv.DictReader(io.StringIO(raw))

    date_field = src.get("date_field", "OrderDate")
    date_fmt = src.get("date_format", "MM-dd-yyyy")

    out_buf = io.StringIO()
    writer = None
    cnt = 0
    for row in reader:
        if _match_run_month(row.get(date_field, ""), date_fmt, run_month):
            if writer is None:
                writer = csv.DictWriter(out_buf, fieldnames=reader.fieldnames)
                writer.writeheader()
            writer.writerow(row)
            cnt += 1

    key = f"{target_prefix}{table}_{run_month}.csv"

    # Idempotencia en Bronze:
    if not allow_overwrite and _object_exists(BUCKET, key):
        _log_table_result(run_id, run_month, table, "SKIPPED_EXISTS", 0, note=f"{key} ya existe", source=source_type)
        return {"table": table, "run_month": run_month, "status": "SKIPPED_EXISTS", "rows": 0, "key": key}

    data = out_buf.getvalue().encode('utf-8')
    _upload_bytes(key, data)

    _log_table_result(run_id, run_month, table, "SUCCEEDED", cnt, note=f"wrote {key}", source=source_type)
    return {"table": table, "run_month": run_month, "status": "SUCCEEDED", "rows": cnt, "key": key}

def _process_dims_once(csv_sources, allow_overwrite, run_id, log_run_month_for_dims):
    """
    Procesa snapshots de dimensiones UNA SOLA VEZ en esta ejecución
    (customers, employees, products, productSubCategories, productCategories, stores, storesBudget, etc.)
    """
    results = []
    for src in csv_sources:
        table = src['table']
        if table == "orders":
            continue  # dims only

        url = src['url_or_query']
        target_prefix = src['target_bronze_prefix']
        source_type = src.get("source", "github")

        # descarga
        with urllib.request.urlopen(url) as resp:
            raw = resp.read().decode('utf-8', errors='replace')
        reader = csv.DictReader(io.StringIO(raw))

        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=reader.fieldnames)
        w.writeheader()
        cnt = 0
        for row in reader:
            w.writerow(row); cnt += 1

        key = f"{target_prefix}{table}.csv"

        if not allow_overwrite and _object_exists(BUCKET, key):
            _log_table_result(run_id, log_run_month_for_dims, table, "SKIPPED_EXISTS", 0, note=f"{key} ya existe", source=source_type)
            results.append({"table": table, "status": "SKIPPED_EXISTS", "rows": 0, "key": key})
            continue

        _upload_bytes(key, buf.getvalue().encode('utf-8'))
        _log_table_result(run_id, log_run_month_for_dims, table, "SUCCEEDED", cnt, note=f"wrote {key}", source=source_type)
        results.append({"table": table, "status": "SUCCEEDED", "rows": cnt, "key": key})

    return results

# ---------- Handler ----------
def handler(event, context):
    """
    Ejemplos de invocación:
    - Un mes:
      { "run_month": "2011-06" }

    - Varios meses (backfill):
      { "run_months": ["2011-06","2011-07","2011-08"] }

    Flags:
      "refresh_dims": false | true        # si true, procesa snapshots de dims 1 vez
      "allow_overwrite": false | true     # si true, reescribe archivos existentes en Bronze
    """
    # Normaliza entrada
    run_months = []
    if "run_months" in event and isinstance(event["run_months"], list):
        run_months = event["run_months"]
    elif "run_month" in event and isinstance(event["run_month"], str):
        run_months = [event["run_month"]]
    else:
        raise ValueError("Debes enviar 'run_month' (string) o 'run_months' (lista de strings YYYY-MM).")

    refresh_dims = bool(event.get("refresh_dims", False))
    allow_overwrite = bool(event.get("allow_overwrite", False))

    run_id = f"csv_{'_'.join(run_months)}_{_ts()}"

    # Carga manifest
    try:
        sources, _defaults = _load_manifest()
    except Exception as e:
        _put_error({
            "run_id": run_id, "ts": _ts(),
            "source": "github", "table": "_manifest", "step": "ingest",
            "severity": "ERROR", "error_code": "LOAD_MANIFEST", "message": str(e)
        })
        raise

    # Filtramos fuentes CSV
    csv_sources = [s for s in sources if s.get("type") == "csv_github"]

    results = {"run_id": run_id, "orders": [], "dims": []}

    # 1) Dims una sola vez (si se pide)
    if refresh_dims:
        # Para logs de dims usamos el primer run_month (o "static" si prefieres)
        log_rm = run_months[0] if run_months else "static"
        try:
            results["dims"] = _process_dims_once(csv_sources, allow_overwrite, run_id, log_rm)
        except Exception as e:
            _put_error({
                "run_id": run_id, "ts": _ts(),
                "source": "github", "table": "_dims", "step": "ingest",
                "severity": "ERROR", "error_code": "DIMS_SNAPSHOT", "message": str(e)
            })
            raise

    # 2) Orders para cada mes solicitado
    for rm in run_months:
        # busca el descriptor de 'orders'
        orders_src = next((s for s in csv_sources if s.get("table") == "orders"), None)
        if not orders_src:
            _put_error({
                "run_id": run_id, "ts": _ts(),
                "source": "github", "table": "orders", "step": "ingest",
                "severity": "ERROR", "error_code": "ORDERS_SRC_NOT_FOUND",
                "message": "No se encontró descriptor 'orders' en el manifest."
            })
            raise RuntimeError("No se encontró descriptor 'orders' en el manifest.")

        try:
            res = _process_orders_month(orders_src, rm, allow_overwrite, run_id)
            results["orders"].append(res)
        except Exception as e:
            _put_error({
                "run_id": run_id, "ts": _ts(),
                "source": orders_src.get("source", "github"),
                "table": "orders", "step": "ingest",
                "severity": "ERROR", "error_code": "ORDERS_MONTH_INGEST", "message": str(e)
            })
            raise

    return results

def lambda_handler(event, context):
    return handler(event, context)
