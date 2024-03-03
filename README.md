# ETL pipeline with Apache Airflow and PostgreSQL database

# Description
This is a simple ETL process orchestrated by Apache Airflow. It imitates unpacking zip archives to a destination folder, transferring unpacked csv files to the PostgreSQL database with `COPY FROM STDOUT` command and performing analytic queries on the transferred data. The process is also supported by logging on two levels:
- text logs available in local (or remote) `logs` directories and Airflow webserver UI
- table log available in `etl.loader_log` in a separate database for storing metadata.

Input folders have the following structure:
```
files
|  - transfer
    |  - <unpack_task_id>
        |  - <source_name>
|
|  - unpack
    | - archive
        |  - <source_name>
    |  - local_files
        |  - <source_name>
    |  - trashbin
```

`dag_unpack_arhives` checks for files in the `./unpack/local_files` directory, unpacks .zip archives to the `./transfer` folder in respect to the source name and the last task id of the unpack task, while copying said archives to the `./unpack/archive` and moving any other file to the `trashbin` (with no respect to the source name).

`dag_transfer_files_to_db` checks for files in the `./transfer/X/S` directories for all sources, where X is the last task id of the unpack task. It then puts found files to PostgreSQL database.

# Installation and Deployment
1. `git clone` or download the repository;
2. Launch Docker, navigate to the project directory with command line and run:
   
   ```
   docker compose up airflow-init
   docker-compose up
   ```

3. Go to http://localhost:8080/home, which is now the Airflow webserver's page;
4. Access remote filesystem of Airflow by clicking *Open in terminal* on any of the currently running Airflow containers (such as `airflow-etl-airflow-worker` or `airflow-etl-airflow-webserver`) in Docker UI;
5. Run:

   `airflow connections import dags/utils/connections.json`
   
   To import all the necessary PostgreSQL connections (you may also add all connections manually: see 7);
6. On the webserver, go to *Admin -> Connections* and make sure all three connections were added;
7. Make sure all four DAGs were loaded on the main page;
8. Enjoy!
