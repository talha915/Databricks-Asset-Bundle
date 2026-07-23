# from framework.config import CATALOG, SILVER_DB
# from framework.utils import get_spark
# from framework.watermark_table import get_latest_watermark, update_watermark

# from .transformation import transform, deduplicate_df

# import pyspark.sql.functions as F

# spark = get_spark()


# def run():
    
#     """
#     Bronze Department -> Silver Department incremental load

#     Read watermark
#     Read new Bronze records
#     Transform
#     Deduplicate
#     Write Silver
#     Update watermark
#     """

#     max_ingestion_time = get_latest_watermark(
#         "bronze_department",
#         "silver.silver_department"
#     )

    
#     df_bronze = (
#         spark.read.table("ai_lab_demo.bronze.department")
#     )

#     if max_ingestion_time is None:
#         unprocessed_df = df_bronze
#         max_ingestion_time = (
#             df_bronze
#             .select(F.max("ingestion_time"))
#             .first()[0]
#         )
        
#     else:
#         unprocessed_df = df_bronze.filter(F.col("ingestion_time") > max_ingestion_time)

#     df_transform = transform(unprocessed_df)

#     if not spark.catalog.tableExists(f"{CATALOG}.{SILVER_DB}.silver_department"):
#         df_transform.write.mode("append").saveAsTable(f"{CATALOG}.{SILVER_DB}.silver_department")
#     else:
#         df_silver = spark.read.table(f"{CATALOG}.{SILVER_DB}.silver_department")
#         df_dedup = deduplicate_df(df_transform, df_silver) 

#         (
#             df_dedup
#             .write
#             .mode("append")
#             .insertInto(f"{CATALOG}.{SILVER_DB}.silver_department")
#         )

    
#     update_watermark(
#         "silver_department",
#         "bronze.bronze_department",
#         "silver.silver_department",
#         max_ingestion_time
#     )    


from framework.config import CATALOG, SILVER_DB
from framework.utils import get_spark
from framework.watermark_table import get_latest_watermark, update_watermark

from .transformation import transform, deduplicate_df

import pyspark.sql.functions as F


spark = get_spark()


def run():

    """
    Bronze Department -> Silver Department incremental load

    Steps:
    1. Read watermark
    2. Read new Bronze records
    3. Calculate new watermark
    4. Transform data
    5. Deduplicate
    6. Write Silver
    7. Update watermark
    """


    # Read last processed watermark
    last_watermark = get_latest_watermark(
        "silver_department",
        "silver.silver_department"
    )


    # Read Bronze table
    df_bronze = spark.read.table(
        "ai_lab_demo.bronze.department"
    )


    # Incremental load
    if last_watermark is None:

        # First run: load everything
        unprocessed_df = df_bronze

    else:

        # Next runs: load only new records
        unprocessed_df = df_bronze.filter(
            F.col("ingestion_time") > last_watermark
        )


    # No new data
    if unprocessed_df.isEmpty():
        print("No new records found")
        return


    # Calculate new watermark from processed data
    new_watermark = (
        unprocessed_df
        .select(
            F.max("ingestion_time")
        )
        .first()[0]
    )


    # Transform Bronze -> Silver
    df_transform = transform(
        unprocessed_df
    )


    silver_table = (
        f"{CATALOG}.{SILVER_DB}.silver_department"
    )


    # First load
    if not spark.catalog.tableExists(
        silver_table
    ):

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
            df_silver
        )


        (
            df_dedup
            .write
            .mode("append")
            .insertInto(silver_table)
        )


    # Update watermark after successful load
    update_watermark(
        "silver_department",
        "bronze.bronze_department",
        "silver.silver_department",
        new_watermark
    )


    print(
        f"Silver load completed. Watermark updated to {new_watermark}"
    )