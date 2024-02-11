'''
DAG used to unpack archives in ./unpack/local_files, move bad files to trashbin,
archive and move good files to ./transfer folder.
'''

import os
import shutil
import zipfile

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

from utils.globals import *
from utils.helpers import *


def unpack_files(**context) -> dict:
    '''
    Create new directories for unpacked files and unpack them, while also archiving them and moving bad files to transhbin.

    Parameters:
        **context : Airflow runtime context.

    Return:
        dict : metadata journal log writable.
    '''
    
    ti = context['ti']
    task_id = ti.xcom_pull(task_ids='get_new_task_id')

    transfer_path = os.path.join(root_path, 'transfer')
    unpack_path = os.path.join(root_path, 'unpack')
    trasbin_path = os.path.join(unpack_path, 'trashbin')

    # Count new files, return early if no new files found
    new_files = 0
    for source in sources:
        local_source_path = os.path.join(unpack_path, 'local_files', source)
        new_files += len(os.listdir(local_source_path))

    if new_files == 0:
        print('No new files found in source folders. Marking as NO DATA')
        return {
            'status' : 'NO DATA',
            'description' : 'No new files found'
        }

    # Create directories
    try:
        for source in sources:
            path = os.path.join(transfer_path, str(task_id), source)
            os.makedirs(path)
        print(f'Successfully created transfer directories for task_id: {task_id}')
    except Exception as ex:
        print(repr(ex))
        return {
            'status' : 'FAILED',
            'description' : 'Error while creating directories'
        }

    # Unpack files
    try:
        for source in sources:
            local_source_path = os.path.join(unpack_path, 'local_files', source)
            archive_path = os.path.join(unpack_path, 'archive', source)

            files = os.listdir(local_source_path)
            for file in files:
                full_path = os.path.join(local_source_path, file)
                if file.endswith('.zip'):
                    with zipfile.ZipFile(full_path, 'r') as zip_file:
                            zip_file.extractall( 
                                path=os.path.join(transfer_path, str(task_id), source)
                        )
                    shutil.move(full_path, os.path.join(archive_path, file))
                    print('Sucessfully unpacked and moved to archive:', file)
                else:
                    shutil.move(full_path, os.path.join(trasbin_path, file))
                    print('Only .zip archives are supported. Failed to unpack and moved to trashbin:', file)
    except Exception as ex:
        print(repr(ex))
        return {
            'status' : 'FAILED',
            'description' : 'Error while unpacking files'
        }
    
    return {
        'status' : 'SUCCESS',
        'description' : ''
    }


with DAG(
    dag_id="dag_unpack_archives",
    start_date=days_ago(1),
    schedule_interval=None,
) as dag:
    get_new_task_id_op = PythonOperator(
        task_id='get_new_task_id',
        python_callable=get_new_task_id,
        provide_context=True
    )

    log_start_op = PostgresOperator(
        task_id='log_start',
        postgres_conn_id=PgConnection.METADATA.value,
        sql='''
            CALL etl.update_log(
                p_task_name => %s::VARCHAR,
                p_task_id => {{ ti.xcom_pull(task_ids='get_new_task_id') }}::INTEGER
            );
        ''',
        parameters=[
            dag.dag_id
        ]
    )

    unpack_files_op = PythonOperator(
        task_id='unpack_files',
        python_callable=unpack_files,
        provide_context=True
    )

    log_end_op = PostgresOperator(
        task_id='log_end',
        postgres_conn_id=PgConnection.METADATA.value,
        sql='''
            CALL etl.update_log(
                p_task_name => %s::VARCHAR,
                p_task_id => {{ ti.xcom_pull(task_ids='get_new_task_id') }}::INTEGER,
                p_status => '{{ ti.xcom_pull(task_ids='unpack_files')['status'] }}',
                p_description => '{{ ti.xcom_pull(task_ids='unpack_files')['description'] }}'
            );
        ''',
        parameters=[
            dag.dag_id
        ]
    )

    get_new_task_id_op >> log_start_op >> unpack_files_op >> log_end_op 
