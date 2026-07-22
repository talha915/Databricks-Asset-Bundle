from framework.config import CATALOG, SILVER_DB
from framework.utils import get_spark
from framework.watermark_table import get_latest_watermark, update_watermark

from .transformation import transform, deduplicate_df, get_unprocessed_bronze_df

import pyspark.sql.functions as F

spark = get_spark()


def run():
    
    """
    Bronze Employees -> Silver Employees incremental load

    Read watermark
    Read new Bronze records
    Transform
    Deduplicate
    Write Silver
    Update watermark
    """

    max_ingestion_time = get_latest_watermark(
        "silver_employee",
        "silver.silver_employee"
    )

    
    df_emp_src_a = (
        spark.read.table("ai_lab_demo.bronze.employees_src_a")
    )

    df_emp_src_b = (
        spark.read.table("ai_lab_demo.bronze.employees_src_b")
    )

    df_emp_src_c = (
        spark.read.table("ai_lab_demo.bronze.employees_src_c")
    )

    if max_ingestion_time is None:
        
        max_ingestion_time_a = (
            df_emp_src_a
            .select(F.max("ingestion_time"))
            .first()[0]
        )
        max_ingestion_time_b = (
            df_emp_src_b
            .select(F.max("ingestion_time"))
            .first()[0]
        )
        max_ingestion_time_c = (
            df_emp_src_c
            .select(F.max("ingestion_time"))
            .first()[0]
        )

        max_ingestion_time = max(max_ingestion_time_a, max_ingestion_time_b, max_ingestion_time_c)
        unprocessed_df_emp_a, unprocessed_df_emp_b, unprocessed_df_emp_c = df_emp_src_a, df_emp_src_b, df_emp_src_c
    else:    
        # Unprocessed bronze rows
        unprocessed_df_emp_a, unprocessed_df_emp_b, unprocessed_df_emp_c = get_unprocessed_bronze_df(df_emp_src_a, df_emp_src_b, df_emp_src_c, max_ingestion_time)

    df_transform = transform(unprocessed_df_emp_a, unprocessed_df_emp_b, unprocessed_df_emp_c)

    df_transform.count()

    if not spark.catalog.tableExists(f"{CATALOG}.{SILVER_DB}.silver_employee"):
        df_transform.write.mode("append").saveAsTable(f"{CATALOG}.{SILVER_DB}.silver_employee")
    else:
        df_silver = spark.read.table(f"{CATALOG}.{SILVER_DB}.silver_employee")
        df_dedup = deduplicate_df(df_transform, df_silver) 

        (
            df_dedup
            .write
            .mode("append")
            .insertInto(f"{CATALOG}.{SILVER_DB}.silver_employee")
        )

    
    update_watermark(
        "silver_employee",
        "[bronze.employee_src_a, bronze.employee_src_b, bronze.employee_src_c]",
        "silver.silver_employee",
        max_ingestion_time
    )    


