from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import pandas as pd

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 4),
    'retries': 1,
}

# List of tables to transfer
tables = [
    'brands', 'categories', 'loyalty_programs', 'suppliers', 'customers', 'products',
    'stores', 'employees', 'payments', 'sales_transactions', 'sales_items', 'inventory',
    'stock_movements', 'purchase_orders', 'shipments', 'returns', 'promotions',
    'campaigns', 'customer_feedback', 'store_visits', 'pricing_history', 'discount_rules',
    'tax_rules'
]

def transfer_table(table_name, **kwargs):
    # Initialize hooks
    mysql_hook = MySqlHook(mysql_conn_id='mysql_connection')
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')

    # Extract data from MySQL
    query = f"SELECT * FROM {table_name}"
    connection = mysql_hook.get_conn()
    df = pd.read_sql(query, connection)

    # Add load_timestamp column with current timestamp
    df['load_timestamp'] = datetime.now()

    df.to_sql(
        "raw_" + table_name,
        postgres_hook.get_sqlalchemy_engine(),
        schema='pos_raw_retails_data',
        if_exists='append',
        index=False
    )

    print(f"Transferred {len(df)} rows from MySQL to PostgreSQL for table {table_name}")

# Initialize DAG
with DAG(
    'mysql_to_postgres',
    default_args=default_args,
    schedule_interval=None,  # Run manually or set a schedule
    catchup=False,
) as dag:

    # --- Task Group for all transfers ---
    with TaskGroup("transfer_group", tooltip="Transfer MySQL tables to Postgres") as transfer_group:
        for table in tables:
            PythonOperator(
                task_id=f'transfer_{table}',
                python_callable=transfer_table,
                op_kwargs={'table_name': table},
            )



