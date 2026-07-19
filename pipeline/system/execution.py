from datetime import datetime
from framework.utils import get_spark

spark = get_spark()


def start_run(layer, job_name):

    run_id = int(datetime.now().timestamp())

    spark.sql(f"""
    INSERT INTO ai_lab_demo.system.execution_log
    VALUES(
        {run_id},
        '{layer}',
        '{job_name}',
        'RUNNING',
        current_timestamp(),
        NULL,
        0,
        NULL
    )
    """)

    return run_id



def end_run(run_id, status, records=0, error=None):

    error_value = "NULL" if error is None else f"'{error}'"

    spark.sql(f"""
    UPDATE ai_lab_demo.system.execution_log

    SET
        status='{status}',
        end_time=current_timestamp(),
        records={records},
        error={error_value}

    WHERE run_id={run_id}

    """)