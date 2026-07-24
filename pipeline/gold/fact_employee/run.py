from framework.config import CATALOG, SILVER_DB, GOLD_DB
from framework.utils import get_spark
from framework.watermark_table import (
    get_latest_watermark,
    update_watermark
)

import pyspark.sql.functions as F


spark = get_spark()


def run():

    """
    Silver Employee -> Gold Fact Employee

    Incremental append load
    """


    fact_table = (
        f"{CATALOG}.{GOLD_DB}.fact_employee"
    )


    # -----------------------------
    # Read watermark
    # -----------------------------

    last_watermark = get_latest_watermark(
        "silver.silver_employee",
        "gold.fact_employee"
    )


    # -----------------------------
    # Read Silver
    # -----------------------------

    df_silver = spark.read.table(
        f"{CATALOG}.{SILVER_DB}.silver_employee"
    )


    # -----------------------------
    # Incremental filter
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
    # Get employee key
    # -----------------------------

    df_dim_employee = (
        spark.table(
            f"{CATALOG}.{GOLD_DB}.dim_employee"
        )
        .filter(
            F.col("is_current") == True
        )
        .select(
            "employee_key",
            "employee_id"
        )
    )


    # -----------------------------
    # Create Fact
    # -----------------------------

    df_fact = (

        df_incremental

        .join(
            df_dim_employee,
            "employee_id",
            "left"
        )

        .select(

            "employee_key",

            "employee_id",

            "salary",

            "currency",

            "department_id",

            "date_of_joining",

            "silver_ingestion_time"

        )

        .withColumn(
            "created_at",
            F.current_timestamp()
        )
    )


    # -----------------------------
    # Write Fact
    # -----------------------------

    (
        df_fact
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(fact_table)
    )


    # -----------------------------
    # Update watermark
    # -----------------------------

    new_watermark = (
        df_incremental
        .select(
            F.max(
                "silver_ingestion_time"
            )
        )
        .first()[0]
    )


    update_watermark(
        "gold_fact_employee",
        "silver.silver_employee",
        "gold.fact_employee",
        new_watermark
    )