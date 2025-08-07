from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 4),
    'retries': 1,
}

# Initialize DAG
dag = DAG(
    'mysql_to_postgres',
    default_args=default_args,
    schedule_interval=None,  # Run manually or set a schedule
    catchup=False,
)

# List of tables to transfer (in order to avoid foreign key issues)
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

    # Load data into PostgreSQL (specify schema 'retail_data')
    # Clear table before loading (optional, remove if you want to append)
    # postgres_hook.run(f"TRUNCATE TABLE retail_data.{table_name} CASCADE")
    df.to_sql("raw_" + table_name, postgres_hook.get_sqlalchemy_engine(), schema='raw_retails_data', if_exists='append', index=False)

    print(f"Transferred {len(df)} rows from MySQL to PostgreSQL for table {table_name} in schema retail_data")

# Create tasks for each table
for table in tables:
    task = PythonOperator(
        task_id=f'transfer_{table}',
        python_callable=transfer_table,
        op_kwargs={'table_name': table},
        dag=dag,
    )
    # Set dependencies to respect foreign key order
    if tables.index(table) > 0:
        previous_task = f'transfer_{tables[tables.index(table) - 1]}'
        dag.get_task(previous_task) >> task




# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from datetime import datetime

# # Danh sách các bảng muốn DROP
# tables = [
#     'brands', 'categories', 'loyalty_programs', 'suppliers', 'customers', 'products',
#     'stores', 'employees', 'payments', 'sales_transactions', 'sales_items', 'inventory',
#     'stock_movements', 'purchase_orders', 'shipments', 'returns', 'promotions',
#     'campaigns', 'customer_feedback', 'store_visits', 'pricing_history', 'discount_rules',
#     'tax_rules'
# ]

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2025, 8, 4),
# }

# dag = DAG(
#     dag_id='drop_postgres_tables',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
#     tags=['postgres', 'drop', 'cleanup'],
# )

# def drop_table(table_name, **kwargs):
#     postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')
#     sql = f'DROP TABLE IF EXISTS {table_name} CASCADE'
#     postgres_hook.run(sql)
#     print(f'Dropped table {table_name}')

# # Tạo task để drop từng bảng
# for table in tables:
#     task = PythonOperator(
#         task_id=f'drop_{table}',
#         python_callable=drop_table,
#         op_kwargs={'table_name': table},
#         dag=dag,
#     )
#     # Đảm bảo thứ tự drop theo foreign key
#     if tables.index(table) > 0:
#         previous_task_id = f'drop_{tables[tables.index(table) - 1]}'
#         dag.get_task(previous_task_id) >> task
