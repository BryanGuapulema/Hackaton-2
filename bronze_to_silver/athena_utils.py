# athena_utils.py
import os, time, boto3

ATHENA = boto3.client("athena")

DB = os.environ["CATALOG_DB"]               # p.ej. hack2_aw_catalog
ATHENA_OUTPUT = os.environ["ATHENA_OUTPUT"] # s3://<bucket>/logs/athena-results/
BUCKET = os.environ["BUCKET_NAME"]          # p.ej. bg-hack2-aw-datalake

def run_athena(sql: str):
    """Ejecuta SQL en Athena y espera a que termine. Lanza error si falla."""
    qid = ATHENA.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT}
    )["QueryExecutionId"]
    while True:
        st = ATHENA.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if st != "SUCCEEDED":
                info = ATHENA.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
                raise RuntimeError(f"Athena {st}: {info.get('StateChangeReason')}")
            return
        time.sleep(1.05)

def ym_from_run_month(run_month: str):
    y_str, m_str = run_month.split("-")
    y = int(y_str); m = int(m_str); m_z = f"{m:02d}"
    return y, m, m_z
