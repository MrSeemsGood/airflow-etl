'''
Globals module.
'''

import enum

class PgConnection(enum.Enum):
    '''
    Enum for PostgreSQL connections.
    Types:

        - INIT - only used to initialize other databases;
        - DATA - used for managing data tables;
        - METADATA - used for managing metadata tables.
    '''
    INIT = 'AIRFLOW_CONN_POSTGRES_INIT'
    DATA = 'AIRFLOW_CONN_POSTGRES_DATA'
    METADATA = 'AIRFLOW_CONN_POSTGRES_META'

root_path = r'/opt/files/'

sources = ['WINE']