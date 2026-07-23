from framework.config import CATALOG, SILVER_DB
from framework.utils import get_spark
from framework.watermark_table import (
    get_latest_watermark,
    update_watermark,
)

from .transformation import (
    transform,
    deduplicate_df,
    get_unprocessed_bronze_df,
)

import pyspark.sql.functions as F

spark = get_spark()


def run():

    """
    Bronze Employees -> Silver Employees incremental load

    Steps:
    1. Read watermark
    2. Read new Bronze records
    3. Calculate new watermark
    4. Transform
    5. Deduplicate
    6. Write Silver
    7. Update watermark
    """

    # Read last processed watermark
    last_watermark = get_latest_watermark(
        "silver_employee",
        "silver.silver_employee"
    )

    # Read Bronze tables
    df_emp_src_a = spark.read.table(
        "ai_lab_demo.bronze.employees_src_a"
    )

    df_emp_src_b = spark.read.table(
        "ai_lab_demo.bronze.employees_src_b"
    )

    df_emp_src_c = spark.read.table(
        "ai_lab_demo.bronze.employees_src_c"
    )

    # Incremental load
    if last_watermark is None:

        # First run
        unprocessed_df_emp_a = df_emp_src_a
        unprocessed_df_emp_b = df_emp_src_b
        unprocessed_df_emp_c = df_emp_src_c

    else:

        # Only new records
        (
            unprocessed_df_emp_a,
            unprocessed_df_emp_b,
            unprocessed_df_emp_c,
        ) = get_unprocessed_bronze_df(
            df_emp_src_a,
            df_emp_src_b,
            df_emp_src_c,
            last_watermark,
        )

    # No new data
    if (
        unprocessed_df_emp_a.isEmpty()
        and unprocessed_df_emp_b.isEmpty()
        and unprocessed_df_emp_c.isEmpty()
    ):
        print("No new employee records found")
        return

    # Calculate new watermark
    max_a = (
        unprocessed_df_emp_a
        .select(F.max("ingestion_time"))
        .first()[0]
    )

    max_b = (
        unprocessed_df_emp_b
        .select(F.max("ingestion_time"))
        .first()[0]
    )

    max_c = (
        unprocessed_df_emp_c
        .select(F.max("ingestion_time"))
        .first()[0]
    )

    new_watermark = max(
        ts for ts in [max_a, max_b, max_c]
        if ts is not None
    )

    # Transform
    df_transform = transform(
        unprocessed_df_emp_a,
        unprocessed_df_emp_b,
        unprocessed_df_emp_c,
    )

    silver_table = (
        f"{CATALOG}.{SILVER_DB}.silver_employee"
    )

    # First load
    if not spark.catalog.tableExists(silver_table):

        (
            df_transform
            .write
            .mode("append")
            .saveAsTable(silver_table)
        )

    # Incremental load
    else:

        df_silver = spark.read.table(
            silver_table
        )

        df_dedup = deduplicate_df(
            df_transform,
            df_silver,
        )

        (
            df_dedup
            .write
            .mode("append")
            .insertInto(silver_table)
        )

    # Update watermark
    update_watermark(
        "silver_employee",
        "[bronze.employee_src_a, bronze.employee_src_b, bronze.employee_src_c]",
        "silver.silver_employee",
        new_watermark,
    )

    print(
        f"Silver employee load completed. Watermark updated to {new_watermark}"
    )