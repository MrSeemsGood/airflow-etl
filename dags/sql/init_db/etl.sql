DROP SCHEMA IF EXISTS etl CASCADE
;

CREATE SCHEMA IF NOT EXISTS etl
;

/*
    Tables and views
*/

CREATE TABLE IF NOT EXISTS etl.loader_log (
    rec_id      SERIAL PRIMARY KEY,
    task_name   VARCHAR,
    task_id     INTEGER,
    start_dttm  TIMESTAMP,
    end_dttm    TIMESTAMP,
    status      VARCHAR,
    description VARCHAR,
    UNIQUE(task_name, task_id)
)
;

/*
    Procedures
*/

CREATE OR REPLACE PROCEDURE etl.update_log
(
    p_task_name VARCHAR,
    p_task_id INTEGER,
    p_dttm TIMESTAMP DEFAULT now(),
    p_status VARCHAR DEFAULT 'SUCCESS',
    p_description VARCHAR DEFAULT ''
)
LANGUAGE 'plpgsql'
AS
$body$
DECLARE
    v_task_exists BOOLEAN;
BEGIN
    SELECT COUNT(*) != 0 INTO v_task_exists
    FROM etl.loader_log 
    WHERE task_name = p_task_name
    AND task_id = p_task_id
    ;

    IF NOT v_task_exists THEN
        -- Add new meta for a new task
        INSERT INTO etl.loader_log (
            task_name,
            task_id,    
            start_dttm,
            status,
            description
        )
        VALUES (
            p_task_name,
            p_task_id,
            p_dttm,
            p_status,
            p_description
        )
        ;
    ELSE
        -- Update meta on existing task
        UPDATE etl.loader_log 
        SET
            end_dttm = p_dttm,
            status = p_status,
            description = p_description
        WHERE task_name = p_task_name
        AND task_id = p_task_id
        ;
    END IF
    ;
END
$body$
;
