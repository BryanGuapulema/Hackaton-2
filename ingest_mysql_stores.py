# lambda_function.py
import os, json, csv, io, boto3, pymysql
from datetime import datetime, timezone

s3 = boto3.client('s3')
secrets = boto3.client('secretsmanager')
dynamodb = boto3.resource('dynamodb')

BUCKET = os.environ['BUCKET_NAME']
CONTROL_TABLE = os.environ['CONTROL_TABLE']
ERROR_TABLE = os.environ['ERROR_TABLE']
SECRET_NAME = os.environ['MYSQL_SECRET_NAME']

def _now_iso(): return datetime.now(timezone.utc).isoformat()

def handler(event, context):
    run_month = event.get("run_month")
    run_id = f"stores_{run_month}_{int(datetime.utcnow().timestamp())}"

    # lee secreto
    sec = json.loads(secrets.get_secret_value(SecretId=SECRET_NAME)['SecretString'])
    conn = pymysql.connect(
        host=sec['host'], user=sec['username'], password=sec['password'],
        database=sec.get('database'), port=int(sec.get('port',3306)),
        connect_timeout=10, cursorclass=pymysql.cursors.DictCursor
    )

    sql = ("SELECT BusinessEntityID AS StoreID, "
           "       Name AS StoreName, "
           "       SalesPersonID AS EmployeeID "
           "FROM Store;")
    with conn.cursor() as cur:
        cur.execute(sql); rows = cur.fetchall()
    conn.close()

    out = io.StringIO()
    fn = ['StoreID','StoreName','EmployeeID']
    w = csv.DictWriter(out, fieldnames=fn); w.writeheader()
    for r in rows: w.writerow(r)

    prefix = f"bronze/source=mysql/table=stores/"
    s3.put_object(Bucket=BUCKET, Key=f"{prefix}stores.csv",
                  Body=out.getvalue().encode('utf-8'))

    dynamodb.Table(CONTROL_TABLE).put_item(Item={
        "run_id": run_id, "run_month": run_month, "source":"mysql", "table":"stores",
        "status":"SUCCEEDED","records_out": len(rows),
        "started_at": _now_iso(), "ended_at": _now_iso()
    })
    return {"run_id": run_id, "records_out": len(rows)}

def lambda_handler(event, context):
    return handler(event, context)
