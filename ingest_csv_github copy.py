import os, json, csv, io, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET = os.environ['BUCKET_NAME']
CONTROL_TABLE = os.environ['CONTROL_TABLE']
ERROR_TABLE = os.environ['ERROR_TABLE']

def _now_iso():
    return datetime.now(timezone.utc).isoformat()

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

def _load_manifest():
    key = "bronze/source_metadata/sources.json"
    body = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
    m = json.loads(body.decode('utf-8'))
    return m.get('sources', []), m.get('run_defaults', {})

def _upload_bytes(key, data: bytes):
    s3.put_object(Bucket=BUCKET, Key=key, Body=data)

def _object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            return False
        raise

def _month_parts(run_month: str):
    y, m = run_month.split("-")
    return int(y), int(m)

def _match_run_month(date_str, pattern, run_month):
    if not date_str:
        return False
    date_str = date_str.strip().split("T")[0].split(" ")[0]

    fmts = []
    # respeta el patrón declarado en sources.json si existe
    if pattern in ("M/d/yyyy", "MM/dd/yyyy"):
        fmts.append("%m/%d/%Y")
    elif pattern in ("MM-dd-yyyy", "M-d-yyyy"):
        fmts.append("%m-%d-%Y")

    # formatos alternativos comunes
    fmts += ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%m/%d/%Y", "%m-%d-%Y"]

    for f in fmts:
        try:
            dt = datetime.strptime(date_str, f)
            y, m = _month_parts(run_month)
            return dt.year == y and dt.month == m
        except Exception:
            continue
    return False

def _clear_prefix(bucket, prefix):
    # borra objetos bajo el prefijo (para overwrite seguro)
    s3r = boto3.resource('s3')
    b = s3r.Bucket(bucket)
    b.objects.filter(Prefix=prefix).delete()

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

def handler(event, context):
    """
    Evento esperado:
      {
        "run_month": "YYYY-MM",
        "refresh_dims": false,        # por defecto: solo orders
        "allow_overwrite": false      # si true, reescribe archivos existentes
      }
    """
    run_month = event.get("run_month")  # "YYYY-MM"
    if not run_month:
        raise ValueError("run_month es requerido (formato YYYY-MM)")

    refresh_dims = bool(event.get("refresh_dims", False))
    allow_overwrite = bool(event.get("allow_overwrite", False))

    run_id = f"csv_{run_month}_{int(datetime.now(timezone.utc).timestamp())}"

    try:
        sources, defaults = _load_manifest()
    except Exception as e:
        _put_error({
            "run_id": run_id, "ts": int(datetime.now(timezone.utc).timestamp()),
            "source": "github", "table": "_manifest", "step": "ingest",
            "severity": "ERROR", "error_code": "LOAD_MANIFEST", "message": str(e)
        })
        raise

    csv_sources = [s for s in sources if s.get("type") == "csv_github"]

    for src in csv_sources:
        table = src['table']  # "orders", "customers", etc.
        url = src['url_or_query']
        target_prefix = src['target_bronze_prefix']  # e.g. bronze/source=github/table=orders/
        source_type = src.get("source", "github")

        # --- Control de qué procesar ---
        if table != "orders" and not refresh_dims:
            _log_table_result(run_id, run_month, table, "SKIPPED", 0, note="refresh_dims=false")
            continue

        try:
            # descarga CSV desde GitHub (o URL que definas)
            with urllib.request.urlopen(url) as resp:
                raw = resp.read().decode('utf-8', errors='replace')
            reader = csv.DictReader(io.StringIO(raw))

            if table == "orders":
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

                # idempotencia en Bronze: si existe y no permites overwrite → SKIP
                if not allow_overwrite and _object_exists(BUCKET, key):
                    _log_table_result(run_id, run_month, table, "SKIPPED_EXISTS", 0, note=f"{key} ya existe")
                    continue

                # si vas a sobreescribir, opcional limpiar prefijo (aquí no es necesario, el key es único por mes)
                data = out_buf.getvalue().encode('utf-8')
                _upload_bytes(key, data)

                _log_table_result(run_id, run_month, table, "SUCCEEDED", cnt, note=f"wrote {key}", source=source_type)

            else:
                # DIMS (snapshot) solo cuando refresh_dims=true
                data_buf = io.StringIO()
                w = csv.DictWriter(data_buf, fieldnames=reader.fieldnames)
                w.writeheader()
                cnt = 0
                for row in reader:
                    w.writerow(row); cnt += 1

                key = f"{target_prefix}{table}.csv"

                if allow_overwrite:
                    # limpio el prefijo completo si quieres un reset total
                    # _clear_prefix(BUCKET, target_prefix)  # opcional
                    pass
                else:
                    # si no vas a sobreescribir y el archivo ya existe → SKIP
                    if _object_exists(BUCKET, key):
                        _log_table_result(run_id, run_month, table, "SKIPPED_EXISTS", 0, note=f"{key} ya existe")
                        continue

                _upload_bytes(key, data_buf.getvalue().encode('utf-8'))
                _log_table_result(run_id, run_month, table, "SUCCEEDED", cnt, note=f"wrote {key}", source=source_type)

        except Exception as e:
            _put_error({
                "run_id": run_id,
                "ts": int(datetime.now(timezone.utc).timestamp()),
                "source": source_type,
                "table": table,
                "step": "ingest",
                "severity": "ERROR",
                "error_code": "CSV_GITHUB_INGEST",
                "message": str(e)
            })
            raise

    return {"run_id": run_id, "run_month": run_month, "refresh_dims": refresh_dims, "allow_overwrite": allow_overwrite}

def lambda_handler(event, context):
    return handler(event, context)
