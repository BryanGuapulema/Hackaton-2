import boto3, time, os

ATHENA = boto3.client("athena")

DB  = os.environ.get("DB", "hack2_aw_catalog")
WG  = os.environ.get("WORKGROUP", "primary")
OUT = os.environ["ATHENA_RESULTS"]  # e.g. s3://bg-hack2-aw-datalake2/athena-results/

def run_athena(sql: str, db: str = DB):
    """
    Ejecuta UNA sentencia SQL en Athena y espera a que termine.
    Devuelve el QueryExecutionId. Lanza excepción si falla.
    """
    qid = ATHENA.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": db},
        WorkGroup=WG,
        ResultConfiguration={"OutputLocation": OUT}
    )["QueryExecutionId"]

    # Espera activa corta
    while True:
        resp = ATHENA.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.3)

    if state != "SUCCEEDED":
        reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        raise RuntimeError(f"Athena failed: {state} - {reason}")

    return qid

def get_scalar_int(sql: str, default: int = 0) -> int:
    """
    Ejecuta una SELECT que devuelve una sola celda numérica (ej: COUNT(*)).
    Retorna int (o 'default' si no hay filas).
    """
    qid = run_athena(sql)
    res = ATHENA.get_query_results(QueryExecutionId=qid)
    rows = res.get("ResultSet", {}).get("Rows", [])
    if not rows:
        return default
    # En Athena, la primera fila ya es dato (no encabezado). Si viniera vacía, devolvemos default.
    first = rows[0].get("Data", [])
    if not first or "VarCharValue" not in first[0]:
        return default
    try:
        return int(float(first[0]["VarCharValue"]))
    except Exception:
        return default
