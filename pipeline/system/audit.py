from framework.utils import get_spark
import uuid

spark = get_spark()


def write_audit(run_id, event, message):

    audit_id = str(uuid.uuid4())

    data = [
        (
            audit_id,
            int(run_id),
            event,
            message
        )
    ]

    schema = """
        audit_id STRING,
        run_id LONG,
        event STRING,
        message STRING
    """

    df = spark.createDataFrame(
        data,
        schema=schema
    )

    df.createOrReplaceTempView("audit_insert")


    spark.sql("""
        INSERT INTO ai_lab_demo.system.audit_log
        SELECT
            audit_id,
            run_id,
            event,
            message,
            current_timestamp()
        FROM audit_insert
    """)