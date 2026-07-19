from framework.utils import get_spark

spark = get_spark()

def write_audit(run_id,event,message):

    spark.sql(f"""

    INSERT INTO audit_log

    VALUES(
    uuid(),
    {run_id},
    '{event}',
    '{message}',
    current_timestamp()
    )

    """)