from framework.config import CATALOG, BRONZE_DB, employees_src_b, employees_src_c
from framework.utils import get_spark
from pyspark.sql.functions import col

spark = get_spark()


def run():
    
    # Checkpoint Path
    checkpoint_path = (
        f"/Volumes/{CATALOG}/{BRONZE_DB}/"
        "pipeline_checkpoints/employees_src_b"
    )

    schema_path = (
        f"/Volumes/{CATALOG}/{BRONZE_DB}/"
        "schema/employees_src_b"
    )

    df_emp_src_b = (
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
        .load(employees_src_b)
    )

    df = df_emp_src_b.select(
        "*",
        col("_metadata.file_path").alias("file_path")
    )


    query = (
        df.writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint_path)
          .option("mergeSchema", "true")
          .trigger(availableNow=True)
          .toTable(
              f"{CATALOG}.{BRONZE_DB}.employees"
          )
    )

    query.awaitTermination()

    progress = query.lastProgress or {}

    rows = progress.get("numInputRows", 0)

    return rows
