# lambda_function.py
from etl_orders import run_orders
from etl_dim_store import run_dim_store
from etl_dim_products import run_dim_products
from etl_dim_customers import run_dim_customers
from etl_dim_employees import run_dim_employees

def handler(event, context):
    run_month = event.get("run_month")  # "YYYY-MM"
    if not run_month:
        raise RuntimeError("Falta run_month (YYYY-MM)")
    refresh_dims = bool(event.get("refresh_dims", False))

    # Hecho (siempre, por mes)
    run_orders(run_month)

    # Dimensiones (solo si se solicita; SCD1)
    if refresh_dims:
        run_dim_store(run_month)
        run_dim_products(run_month)
        run_dim_customers(run_month)
        run_dim_employees(run_month)

    return {"status": "SUCCEEDED", "run_month": run_month, "refresh_dims": refresh_dims}

def lambda_handler(event, context):
    return handler(event, context)
