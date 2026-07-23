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
    Silver Department -> Gold Dimension Department

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
        f"{CATALOG}.{GOLD_DB}.dim_department"
    )


    # --------------------------------
    # Read Gold watermark
    # --------------------------------

    max_ingestion_time = get_latest_watermark(
        "silver.silver_department",
        "gold.dim_department"
    )


    # --------------------------------
    # Read Silver
    # --------------------------------

    df_silver = spark.read.table(
        f"{CATALOG}.{SILVER_DB}.silver_department"
    )


    # --------------------------------
    # Incremental filter
    # --------------------------------

    if max_ingestion_time is None:

        df_incremental = df_silver


    else:

        df_incremental = (
            df_silver
            .filter(
                F.col("ingestion_time") > max_ingestion_time
            )
        )


    if df_incremental.isEmpty():
        return


    # --------------------------------
    # Transform SCD2
    # --------------------------------

    df_changes = transform(df_incremental)


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
                target.department_id =
                source.department_id

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


    # --------------------------------
    # Update watermark
    # --------------------------------

    max_ingestion_time = (
        df_incremental
        .select(
            F.max("ingestion_time")
        )
        .first()[0]
    )


    update_watermark(
        "gold_department",
        "silver.silver_department",
        "gold.dim_department",
        max_ingestion_time
    )