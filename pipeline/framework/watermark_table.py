from framework.utils import get_spark
import pyspark.sql.functions as F
from datetime import datetime

spark = get_spark()

def get_latest_watermark(
    pipeline_name: str, 
    pipeline_destination: str
) -> str:

    result = spark.sql(f"""
        SELECT ingestion_time
        FROM ai_lab_demo.system.watermark
        WHERE pipeline_name = '{pipeline_name}'
        AND pipeline_destination = '{pipeline_destination}'
        ORDER BY updated_at DESC
        LIMIT 1
    """).collect()

    if len(result) == 0:
        return None

    return result[0][0]


def update_watermark(
    pipeline_name, 
    pipeline_src,
    pipeline_destination, 
    ingestion_time
):
    """
    Updating the watermark in the table
    """
    df = spark.read.table("ai_lab_demo.system.watermark")
    df_filter = (
        df
        .filter(
            (F.col("pipeline_name") == pipeline_name) &
            (F.col("pipeline_destination") == pipeline_destination)
        )
    )

    if df_filter.isEmpty():
        data = [
            (
                pipeline_name,
                pipeline_src,
                pipeline_destination,
                ingestion_time,
                datetime.now()
            )
        ]

        df = spark.createDataFrame(
            data,
            [
                "pipeline_name",
                "pipeline_src",
                "pipeline_destination",
                "ingestion_time",
                "updated_at"
            ]
        )
        df.write.mode("append").saveAsTable("ai_lab_demo.system.watermark")

    else:
        spark.sql(f"""
            UPDATE ai_lab_demo.system.watermark
            SET
                ingestion_time = '{ingestion_time}',
                updated_at = current_timestamp()
            WHERE pipeline_name = '{pipeline_name}' and pipeline_destination = '{pipeline_destination}'
        """)
    