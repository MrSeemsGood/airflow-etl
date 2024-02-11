'''
DAG used to initialize all necessary databases (for managing data and metadata associated with ETL pipeline).
'''

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.globals import PgConnection


with DAG(
    dag_id="dag_init_db",
    start_date=days_ago(1),
    schedule_interval=None,
) as dag:
    drop_data_db = PostgresOperator(
        task_id="drop_data_db",
        postgres_conn_id=PgConnection.INIT.value,
        sql="""
            DROP DATABASE IF EXISTS data;
        """,
        autocommit=True
    )

    init_data_db = PostgresOperator(
        task_id="init_data_db",
        postgres_conn_id=PgConnection.INIT.value,
        sql="""
            CREATE DATABASE data
                WITH OWNER = airflow;
        """,
        autocommit=True
    )

    drop_meta_db = PostgresOperator(
        task_id="drop_meta_db",
        postgres_conn_id=PgConnection.INIT.value,
        sql="""
            DROP DATABASE IF EXISTS etl;
        """,
        autocommit=True
    )

    init_meta_db = PostgresOperator(
        task_id="init_meta_db",
        postgres_conn_id=PgConnection.INIT.value,
        sql="""
            CREATE DATABASE etl
                WITH OWNER = airflow;
        """,
        autocommit=True
    )

    init_data_schemas = PostgresOperator(
        task_id="init_data_schemas",
        postgres_conn_id=PgConnection.DATA.value,
        sql='./sql/init_db/data.sql'
    )

    init_meta_schemas = PostgresOperator(
        task_id="init_meta_schemas",
        postgres_conn_id=PgConnection.METADATA.value,
        sql='./sql/init_db/etl.sql'
    )

    drop_data_db >> init_data_db >> drop_meta_db >> init_meta_db >> init_data_schemas >> init_meta_schemas
