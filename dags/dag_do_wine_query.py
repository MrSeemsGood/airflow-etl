'''
DAG used to perform queries on files uploaded to the database.
'''

import os

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from utils.globals import PgConnection


def do_wine_query():
    '''
    Perform query on a data.

    Return:
        dict : metadata journal log writable.
    '''
    hook = PostgresHook(PgConnection.DATA.value)

    #? Workaround since there doesn't seem to be a way of
    #? passing .sql files into PostgresHook
    with open(os.path.join(os.getcwd(), 'dags', 'sql', 'test_sql_wine.sql')) as sql_file:
        query = sql_file.read() 

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            print('Executing queries:', query[:100], '...')
            try:
                cursor.execute(query)
                return {
                    'status' : 'SUCCESS',
                    'description' : ''
                }
            except Exception as ex:
                print(repr(ex))
                return {
                    'status' : 'FAILED',
                    'description' : 'Error while executing queries'
                }


with DAG(
    dag_id="dag_do_wine_query",
    start_date=days_ago(1),
    schedule_interval=None,
) as dag:
    log_start_op = PostgresOperator(
        task_id='log_start',
        postgres_conn_id=PgConnection.METADATA.value,
        sql='''
            CALL etl.update_log(
                p_task_name => %s::VARCHAR,
                p_task_id => {{ ti.xcom_pull(task_ids='get_new_task_id', dag_id='dag_unpack_archives', include_prior_dates=True) }}::INTEGER
            );
        ''',
        parameters=[
            dag.dag_id
        ]
    )

    do_wine_query_op = PythonOperator(
        task_id='do_wine_query',
        python_callable=do_wine_query,
        provide_context=True
    )

    log_end_op = PostgresOperator(
        task_id='log_end',
        postgres_conn_id=PgConnection.METADATA.value,
        sql='''
            CALL etl.update_log(
                p_task_name => %s::VARCHAR,
                p_task_id => {{ ti.xcom_pull(task_ids='get_new_task_id', dag_id='dag_unpack_archives', include_prior_dates=True) }}::INTEGER,
                p_status => '{{ ti.xcom_pull(task_ids='do_wine_query')['status'] }}',
                p_description => '{{ ti.xcom_pull(task_ids='do_wine_query')['description'] }}'
            );
        ''',
        parameters=[
            dag.dag_id
        ]
    )

    log_start_op >> do_wine_query_op >> log_end_op
