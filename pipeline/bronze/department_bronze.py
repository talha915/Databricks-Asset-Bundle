from framework.config import CATALOG, BRONZE_DB, departments
from framework.utils import get_spark
from pyspark.sql.functions import col
import pyspark.sql.functions as F

spark = get_spark()


def run():
    
    # Checkpoint Path
    checkpoint_path = (
        f"/Volumes/{CATALOG}/{BRONZE_DB}/"
        "pipeline_checkpoints/department"
    )

    schema_path = (
        f"/Volumes/{CATALOG}/{BRONZE_DB}/"
        "schema/department"
    )

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option(
             "cloudFiles.schemaLocation",
             schema_path
         )
         .option(
             "cloudFiles.schemaEvolutionMode",
             "addNewColumns"
         )
        .option("header", "true")
        .load(departments)
    )

    df = (
        df
        .select("*",col("_metadata.file_path").alias("file_path"))
        .withColumn("ingestion_time", F.current_timestamp())
    )


    query = (
        df.writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint_path)
          .option("mergeSchema", "true")
          .trigger(availableNow=True)
          .toTable(
              f"{CATALOG}.{BRONZE_DB}.department"
          )
    )

    query.awaitTermination()

    progress = query.lastProgress or {}

    rows = progress.get("numInputRows", 0)

    return rows
