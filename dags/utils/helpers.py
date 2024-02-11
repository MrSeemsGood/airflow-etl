'''
Utils module.
'''

from utils.globals import PgConnection
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_new_task_id(task_name:str=None) -> int:
    '''
    Get new task id.

    Params:
        task_name str : name of the task. Can be unspecified (`None`) to find max id for all tasks.

    Return:
        int : task id.
    '''
    hook = PostgresHook(PgConnection.METADATA.value)
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            try:
                if task_name is None:
                    cursor.execute('''SELECT MAX(task_id)
                                FROM etl.loader_log
                                ''')
                else:
                    cursor.execute('''SELECT MAX(task_id)
                                FROM etl.loader_log
                                WHERE task_name = %s
                                ''',
                                vars=[task_name])
                max_task_id = int(cursor.fetchone()[0])
            except:
                max_task_id = 0

    return max_task_id + 1
