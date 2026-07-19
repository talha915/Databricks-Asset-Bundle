from framework.utils import get_spark
import uuid

spark = get_spark()

def write_audit(run_id,event,message):

    audit_id = str(uuid.uuid4())

    spark.sql(f"""

    INSERT INTO ai_lab_demo.system.audit_log

    VALUES(
    '{audit_id}',
    {run_id},
    '{event}',
    '{message}',
    current_timestamp()
    )

    """)