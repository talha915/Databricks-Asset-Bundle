import sys
sys.path.append("../")

from src.spark_utils import get_spark, save_delta
from src.monitoring import create_metric
from src.metadata_reader import read_yaml
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def run_bronze_ingestion():
    try:
        spark=get_spark()
        
        config = read_yaml(
            "../metadata/bronze_config.yaml"
        )

        source_path=config["source"]["path"]

        df=spark.read \
        .option(
            "header",
            config["options"]["header"]
        )\
        .option(
            "inferSchema",
            config["options"]["inferSchema"]
        )\
        .csv(source_path)


        save_delta(
            df,
            config["target"]["database"],
            config["target"]["table"]
        )


        metric=create_metric(
            "bronze_ingestion",
            "SUCCESS",
            df.count()
        )


    except Exception as e:


        metric=create_metric(
            "bronze_ingestion",
            "FAILED",
            0,
            str(e)
        )


    # Convert datetime to string for DataFrame creation
    metric_record = metric.copy()
    metric_record["run_time"] = str(metric_record["run_time"])
    
    # Define explicit schema for monitoring table
    schema = StructType([
        StructField("job_name", StringType(), True),
        StructField("run_time", StringType(), True),
        StructField("status", StringType(), True),
        StructField("records_processed", IntegerType(), True),
        StructField("error", StringType(), True)
    ])
    
    spark.createDataFrame(
        [metric_record], schema=schema
    ).write.mode("append") \
    .saveAsTable(
        "ai_lab_demo.monitoring.pipeline_logs"
    )