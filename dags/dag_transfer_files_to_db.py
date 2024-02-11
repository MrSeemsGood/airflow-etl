'''
DAG used to transfer unpacked .csv files from ./transfer to the PostgreSQL database with respect to the # of
the last load task.
'''

import os

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from utils.globals import *
from utils.helpers import *


source_to_filename = {
    'WINE' : {
        'winequality-red.csv' : 'red_wine',
        'winequality-white.csv' : 'white_wine'
    }
}


def transfer_files(**context) -> dict:
    '''
    Transfer newly unpacked .csv files to database.

    Parameters:
        **context : Airflow runtime context.

    Return:
        dict : metadata journal log writable.
    '''

    ti = context['ti']
    task_id = ti.xcom_pull(task_ids='get_new_task_id', dag_id='dag_unpack_archives', include_prior_dates=True)
    transfer_path = os.path.join(root_path, 'transfer')
    
    if not os.path.exists(os.path.join(transfer_path, str(task_id))):
        print('No new unpacked files for this task id were found for transfer. Marking as NO DATA')
        return {
            'status' : 'NO DATA',
            'description' : 'Nothing was unpacked in this task'
        }

    try:
        pg_hook = PostgresHook.get_hook(PgConnection.DATA.value)
        for source in sources:
            for fn, tn in source_to_filename[source].items():
                print(f'Transferring file: {task_id}/{source}/{fn} to database')
                pg_hook.copy_expert(
                    sql=f'''
                        COPY wine_quality.{tn} FROM STDOUT DELIMITER ';' CSV HEADER
                        ;
                    ''',
                    filename=os.path.join(transfer_path, str(task_id), source, fn)
                )

        return {
            'status' : 'SUCCESS',
            'description' : ''
        }
    except Exception as ex:
        print(repr(ex))
        return {
            'status' : 'FAILED',
            'description' : 'Error while transferring files'
        }


with DAG(
    dag_id="dag_transfer_files_to_db",
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

    transfer_files_op = PythonOperator(
        task_id='transfer_files',
        python_callable=transfer_files,
        provide_context=True
    )

    log_end_op = PostgresOperator(
        task_id='log_end',
        postgres_conn_id=PgConnection.METADATA.value,
        sql='''
            CALL etl.update_log(
                p_task_name => %s::VARCHAR,
                p_task_id => {{ ti.xcom_pull(task_ids='get_new_task_id', dag_id='dag_unpack_archives', include_prior_dates=True) }}::INTEGER,
                p_status => '{{ ti.xcom_pull(task_ids='transfer_files')['status'] }}',
                p_description => '{{ ti.xcom_pull(task_ids='transfer_files')['description'] }}'
            );
        ''',
        parameters=[
            dag.dag_id
        ]
    )

    log_start_op >> transfer_files_op >> log_end_op 
