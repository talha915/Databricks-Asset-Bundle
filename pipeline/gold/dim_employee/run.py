from framework.config import CATALOG, SILVER_DB, GOLD_DB
from framework.utils import get_spark
from framework.watermark_table import (
    get_latest_watermark,
    update_watermark
)

from .transformation import transform

from delta.tables import DeltaTable

import pyspark.sql.functions as F


spark = get_spark()


def run():

    """
    Silver Employee -> Gold Dimension Employee

    Incremental SCD Type 2 load

    Watermark:
        Silver ingestion_time

    Steps:
    1. Read watermark
    2. Read new Silver records
    3. Transform for SCD2
    4. Expire old records
    5. Insert new versions
    6. Update watermark
    """


    gold_table = (
        f"{CATALOG}.{GOLD_DB}.dim_employee"
    )


    # --------------------------------
    # Read Gold watermark
    # --------------------------------

    last_watermark = get_latest_watermark(
        "silver.silver_employee",
        "gold.dim_employee"
    )


    # --------------------------------
    # Read Silver
    # --------------------------------

    df_silver = spark.read.table(
        f"{CATALOG}.{SILVER_DB}.silver_employee"
    )


    # --------------------------------
    # Incremental filter
    # --------------------------------

    if last_watermark is None:

        df_incremental = df_silver


    else:

        df_incremental = (
            df_silver
            .filter(
                F.col("silver_ingestion_time") > last_watermark
            )
        )


    if df_incremental.isEmpty():
        return


    # --------------------------------
    # Transform SCD2
    # --------------------------------

    # Deduplicate by employee_id, keeping latest record
    from pyspark.sql.window import Window
    
    window_spec = (
        Window
        .partitionBy("employee_id")
        .orderBy(F.col("silver_ingestion_time").desc())
    )
    
    df_deduplicated = (
        df_incremental
        .withColumn(
            "row_num",
            F.row_number().over(window_spec)
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    df_changes = transform(df_deduplicated)


    # latest watermark after successful processing
    new_watermark = (
        df_incremental
        .select(
            F.max("silver_ingestion_time")
        )
        .first()[0]
    )


    # --------------------------------
    # First load
    # --------------------------------

    if not spark.catalog.tableExists(gold_table):

        (
            df_changes
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(gold_table)
        )


    else:


        delta_dim = DeltaTable.forName(
            spark,
            gold_table
        )


        # --------------------------------
        # Step 1:
        # Expire old current records
        # --------------------------------

        (
            delta_dim.alias("target")
            .merge(
                df_changes.alias("source"),
                """
                target.employee_id =
                source.employee_id

                AND target.is_current = true
                """
            )
            .whenMatchedUpdate(
                condition="""
                target.hash_value <>
                source.hash_value
                """,
                set={

                    "end_date":
                        "current_date()",

                    "is_current":
                        "false",

                    "updated_at":
                        "current_timestamp()"
                }
            )
            .execute()
        )


        # --------------------------------
        # Step 2:
        # Insert new records
        # --------------------------------

        (
            df_changes
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(gold_table)
        )


    update_watermark(
        "gold_employee",
        "silver.silver_employee",
        "gold.dim_employee",
        new_watermark
    )