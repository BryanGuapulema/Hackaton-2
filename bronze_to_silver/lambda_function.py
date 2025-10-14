from etl_orders import run_orders
from etl_dim_store import run_dim_store
from etl_dim_products import run_dim_products
from etl_dim_customers import run_dim_customers
from etl_dim_employees import run_dim_employees
import json, logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def handler(event, context):
    log.info(f"EVENT: {json.dumps(event)}")
    run_month = event.get("run_month")  # "YYYY-MM"
    if not run_month:
        raise RuntimeError("Falta run_month (YYYY-MM)")
    refresh_dims = bool(event.get("refresh_dims", False))

    # Siempre: fact (orders) del mes indicado
    run_orders(run_month)

    # Dimensiones (SCD1 snapshot por run_month)
    if refresh_dims:
        run_dim_store(run_month)
        run_dim_products(run_month)
        run_dim_customers(run_month)
        run_dim_employees(run_month)

    result = {"status": "SUCCEEDED", "run_month": run_month, "refresh_dims": refresh_dims}
    log.info(f"RESULT: {json.dumps(result)}")
    return result

def lambda_handler(event, context):
    return handler(event, context)
