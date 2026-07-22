from framework.config import CATALOG, BRONZE_DB, employees_src_c
from framework.utils import get_spark
from pyspark.sql.functions import col
import pyspark.sql.functions as F

spark = get_spark()


def run():
    
    # Checkpoint Path
    checkpoint_path = (
        f"/Volumes/{CATALOG}/{BRONZE_DB}/"
        "pipeline_checkpoints/employees_src_c"
    )

    schema_path = (
        f"/Volumes/{CATALOG}/{BRONZE_DB}/"
        "schema/employees_src_c"
    )

    df_emp_src_c = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option(
             "cloudFiles.schemaLocation",
             schema_path
         )
         .option(
             "cloudFiles.schemaEvolutionMode",
             "addNewColumns"
         )
        .option("rowTag", "record") 
        .load(employees_src_c)
    )

    df = (
        df_emp_src_c
        .select(
            "*",
            col("_metadata.file_path").alias("file_path")
        )
        .withColumn("ingestion_time", F.current_timestamp())
    )


    query = (
        df.writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint_path)
          .option("mergeSchema", "true")
          .trigger(availableNow=True)
          .toTable(
              f"{CATALOG}.{BRONZE_DB}.employees_src_c"
          )
    )

    query.awaitTermination()

    progress = query.lastProgress or {}

    rows = progress.get("numInputRows", 0)

    return rows
