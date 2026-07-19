from datetime import datetime
from framework.utils import get_spark

spark = get_spark()

def start_run(layer, job_name):

    run_id = int(datetime.now().timestamp())

    spark.sql(f"""
    INSERT INTO execution_logs
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

    spark.sql(f"""
    UPDATE execution_logs

    SET
    status='{status}',
    end_time=current_timestamp(),
    records={records},
    error='{error}'

    WHERE run_id={run_id}

    """)