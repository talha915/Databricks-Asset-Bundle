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

    gold_table = (
        f"{CATALOG}.{GOLD_DB}.dim_department"
    )


    # -----------------------------
    # Read watermark
    # -----------------------------

    last_watermark = get_latest_watermark(
        "silver.silver_department",
        "gold.dim_department"
    )


    # -----------------------------
    # Read Silver
    # -----------------------------

    df_silver = spark.read.table(
        f"{CATALOG}.{SILVER_DB}.silver_department"
    )


    # -----------------------------
    # Incremental load
    # -----------------------------

    if last_watermark is None:

        df_incremental = df_silver

    else:

        df_incremental = (
            df_silver
            .filter(
                F.col("silver_ingestion_time")
                > last_watermark
            )
        )


    if df_incremental.isEmpty():
        return


    # -----------------------------
    # Transform SCD2
    # -----------------------------

    df_changes = transform(
        df_incremental
    )


    # latest watermark after successful processing
    new_watermark = (
        df_incremental
        .select(
            F.max("silver_ingestion_time")
        )
        .first()[0]
    )


    # -----------------------------
    # First load
    # -----------------------------

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


        # -----------------------------
        # Expire old records
        # -----------------------------

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
                    "is_current": "false",
                    "end_date": "current_date()",
                    "updated_at": "current_timestamp()"
                }
            )
            .execute()
        )


        # -----------------------------
        # Insert new records
        # -----------------------------

        (
            df_changes
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(gold_table)
        )


    # -----------------------------
    # Update watermark LAST
    # -----------------------------

    update_watermark(
        "gold_department",
        "silver.silver_department",
        "gold.dim_department",
        new_watermark
    )