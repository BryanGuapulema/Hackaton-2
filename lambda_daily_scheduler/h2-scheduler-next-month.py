import os, json, boto3

dynamodb = boto3.resource('dynamodb')
lambda_client = boto3.client('lambda')

CONTROL_TABLE = os.environ['CONTROL_TABLE']
TARGET_INGEST_LAMBDA = os.environ['TARGET_INGEST_LAMBDA']
ORDERS_TABLE_NAME = os.environ.get('ORDERS_TABLE_NAME', 'orders')
START_MONTH = os.environ.get('START_MONTH', '2011-05')
END_MONTH = os.environ.get('END_MONTH', '2014-05')  # límite duro

def _parse_ym(ym: str):
    y, m = ym.split('-')
    return int(y), int(m)

def _fmt_ym(y: int, m: int):
    return f"{y:04d}-{m:02d}"

def _next_month(ym: str) -> str:
    y, m = _parse_ym(ym)
    return _fmt_ym(y + (1 if m == 12 else 0), 1 if m == 12 else m + 1)

def _ym_le(a: str, b: str) -> bool:
    ay, am = _parse_ym(a); by, bm = _parse_ym(b)
    return (ay, am) <= (by, bm)

def _get_last_succeeded_run_month():
    """
    Escanea etl_control y devuelve el max run_month para table='orders' y status='SUCCEEDED'.
    OJO: Scan no está ordenado, por eso calculamos el max manualmente.
    """
    table = dynamodb.Table(CONTROL_TABLE)
    from boto3.dynamodb.conditions import Attr

    fe = Attr('table').eq(ORDERS_TABLE_NAME) & Attr('status').eq('SUCCEEDED')
    run_months = []
    last_evaluated_key = None

    while True:
        kwargs = {
            'ProjectionExpression': 'run_month, #tbl, #st',
            'FilterExpression': fe,
            'ExpressionAttributeNames': {'#tbl':'table', '#st':'status'}
        }
        if last_evaluated_key:
            kwargs['ExclusiveStartKey'] = last_evaluated_key

        resp = table.scan(**kwargs)
        for it in resp.get('Items', []):
            rm = it.get('run_month')
            if rm:
                run_months.append(rm)

        last_evaluated_key = resp.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break

    return max(run_months) if run_months else None

def handler(event, context):
    # 1) último mes exitoso
    last_ok = _get_last_succeeded_run_month()

    # 2) candidato
    candidate = _next_month(last_ok) if last_ok else START_MONTH

    # 3) respetar límite 2014-05
    if not _ym_le(candidate, END_MONTH):
        msg = {
            "status": "NO_ACTION",
            "reason": f"candidate {candidate} > END_MONTH {END_MONTH}"
        }
        print(json.dumps(msg))
        return msg

    # 4) invocar ingest_csv_github con el formato que usas
    payload = {"run_months": [candidate]}
    resp = lambda_client.invoke(
        FunctionName=TARGET_INGEST_LAMBDA,
        InvocationType='Event',  # async
        Payload=json.dumps(payload).encode('utf-8')
    )

    out = {
        "status": "INVOKED",
        "target_lambda": TARGET_INGEST_LAMBDA,
        "run_months": payload["run_months"],
        "invoke_http_status": resp['StatusCode']
    }
    print(json.dumps(out))
    return out

def lambda_handler(event, context):
    return handler(event, context)



# ###
# Rol (permisos mínimos):

# AWSLambdaBasicExecutionRole (logs)

# AmazonDynamoDBReadOnlyAccess (o permisos finos de dynamodb:Scan a etl_control)

# Permiso para invocar la Lambda: lambda:InvokeFunction sobre ingest_csv_github

# {
#   "Version": "2012-10-17",
#   "Statement": [
#     { "Effect": "Allow",
#       "Action": ["lambda:InvokeFunction"],
#       "Resource": "arn:aws:lambda:us-east-1:255731431472:function:ingest_csv_github"
#     }
#   ]
# }
