from datetime import datetime
from framework.utils import get_spark
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType
)
from pyspark.sql.functions import current_timestamp

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

    records = 0 if records is None else records

    schema = StructType([
        StructField("run_id", LongType(), False),
        StructField("status", StringType(), False),
        StructField("records", LongType(), False),
        StructField("error", StringType(), True)
    ])

    data = [
        (
            int(run_id),
            status,
            int(records),
            error
        )
    ]

    df = spark.createDataFrame(
        data,
        schema=schema
    )

    df.createOrReplaceTempView("run_update")

    spark.sql("""
        MERGE INTO ai_lab_demo.system.execution_log t
        USING run_update s
        ON t.run_id = s.run_id

        WHEN MATCHED THEN UPDATE SET
            t.status = s.status,
            t.end_time = current_timestamp(),
            t.records = s.records,
            t.error = s.error
    """)
# def end_run(run_id, status, records=0, error=None):

#     error_value = "NULL" if error is None else f"'{error}'"

#     spark.sql(f"""
#     UPDATE ai_lab_demo.system.execution_log

#     SET
#         status='{status}',
#         end_time=current_timestamp(),
#         records={records},
#         error={error_value}

#     WHERE run_id={run_id}

#     """)