# from framework.config import CATALOG, SILVER_DB, GOLD_DB
# from framework.utils import get_spark
# from framework.watermark_table import (
#     get_latest_watermark,
#     update_watermark
# )

# from .transformation import transform

# from delta.tables import DeltaTable

# import pyspark.sql.functions as F


# spark = get_spark()


# def run():

#     """
#     Silver Employee -> Gold Dimension Employee

#     Incremental SCD Type 2 load

#     Watermark:
#         Silver ingestion_time

#     Steps:
#     1. Read watermark
#     2. Read new Silver records
#     3. Transform for SCD2
#     4. Expire old records
#     5. Insert new versions
#     6. Update watermark
#     """


#     gold_table = (
#         f"{CATALOG}.{GOLD_DB}.dim_employee"
#     )


#     # --------------------------------
#     # Read Gold watermark
#     # --------------------------------

#     last_watermark = get_latest_watermark(
#         "silver.silver_employee",
#         "gold.dim_employee"
#     )


#     # --------------------------------
#     # Read Silver
#     # --------------------------------

#     df_silver = spark.read.table(
#         f"{CATALOG}.{SILVER_DB}.silver_employee"
#     )


#     # --------------------------------
#     # Incremental filter
#     # --------------------------------

#     if last_watermark is None:

#         df_incremental = df_silver


#     else:

#         df_incremental = (
#             df_silver
#             .filter(
#                 F.col("silver_ingestion_time") > last_watermark
#             )
#         )


#     if df_incremental.isEmpty():
#         return


#     # --------------------------------
#     # Transform SCD2
#     # --------------------------------

#     df_changes = transform(df_incremental)


#     # latest watermark after successful processing
#     new_watermark = (
#         df_incremental
#         .select(
#             F.max("silver_ingestion_time")
#         )
#         .first()[0]
#     )


#     # --------------------------------
#     # First load
#     # --------------------------------

#     if not spark.catalog.tableExists(gold_table):

#         (
#             df_changes
#             .write
#             .format("delta")
#             .mode("overwrite")
#             .saveAsTable(gold_table)
#         )


#     else:


#         delta_dim = DeltaTable.forName(
#             spark,
#             gold_table
#         )


#         # --------------------------------
#         # Step 1:
#         # Expire old current records
#         # --------------------------------

#         (
#             delta_dim.alias("target")
#             .merge(
#                 df_changes.alias("source"),
#                 """
#                 target.employee_id =
#                 source.employee_id

#                 AND target.is_current = true
#                 """
#             )
#             .whenMatchedUpdate(
#                 condition="""
#                 target.hash_value <>
#                 source.hash_value
#                 """,
#                 set={

#                     "end_date":
#                         "current_date()",

#                     "is_current":
#                         "false",

#                     "updated_at":
#                         "current_timestamp()"
#                 }
#             )
#             .execute()
#         )


#         # --------------------------------
#         # Step 2:
#         # Insert new records
#         # --------------------------------

#         (
#             df_changes
#             .write
#             .format("delta")
#             .mode("append")
#             .saveAsTable(gold_table)
#         )


#     update_watermark(
#         "gold_employee",
#         "silver.silver_employee",
#         "gold.dim_employee",
#         new_watermark
#     )



from framework.config import CATALOG, SILVER_DB, GOLD_DB
from framework.utils import get_spark
from framework.watermark_table import (
    get_latest_watermark,
    update_watermark
)

from .transformation import transform

from delta.tables import DeltaTable

import pyspark.sql.functions as F
from pyspark.sql.window import Window


spark = get_spark()


def run():

    """
    Silver Employee -> Gold Dimension Employee

    Incremental SCD Type 2 Load

    Flow:

    1. Read watermark
    2. Read new Silver records
    3. Keep latest record per employee
    4. Generate hash
    5. Compare with current Gold
    6. Expire changed records
    7. Insert new versions
    8. Update watermark
    """


    gold_table = (
        f"{CATALOG}.{GOLD_DB}.dim_employee"
    )


    # --------------------------------------
    # Read watermark
    # --------------------------------------

    last_watermark = get_latest_watermark(
        "silver.silver_employee",
        "gold.dim_employee"
    )


    # --------------------------------------
    # Read Silver
    # --------------------------------------

    df_silver = spark.read.table(
        f"{CATALOG}.{SILVER_DB}.silver_employee"
    )


    # --------------------------------------
    # Incremental filter
    # --------------------------------------

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


    # --------------------------------------
    # Keep latest employee record only
    # FIX DUPLICATE MERGE MATCH
    # --------------------------------------

    latest_window = (
        Window
        .partitionBy(
            "employee_id"
        )
        .orderBy(
            F.col(
                "silver_ingestion_time"
            ).desc()
        )
    )


    df_incremental = (
        df_incremental
        .withColumn(
            "rn",
            F.row_number()
            .over(latest_window)
        )
        .filter(
            F.col("rn") == 1
        )
        .drop("rn")
    )


    # --------------------------------------
    # Transform
    # --------------------------------------

    df_changes = transform(
        df_incremental
    )


    # --------------------------------------
    # First load
    # --------------------------------------

    if not spark.catalog.tableExists(
        gold_table
    ):


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


        # ----------------------------------
        # Current Gold records
        # ----------------------------------

        df_current = (
            spark.table(gold_table)
            .filter(
                F.col("is_current") == True
            )
            .select(
                "employee_id",
                "hash_value"
            )
        )


        # ----------------------------------
        # Only new or changed employees
        # ----------------------------------

        df_changes = (

            df_changes.alias("source")

            .join(
                df_current.alias("target"),
                "employee_id",
                "left"
            )

            .filter(
                """
                target.employee_id IS NULL
                OR source.hash_value <> target.hash_value
                """
            )

            .select(
                "source.*"
            )

        )


        if df_changes.isEmpty():

            update_watermark(
                "gold_employee",
                "silver.silver_employee",
                "gold.dim_employee",
                (
                    df_incremental
                    .select(
                        F.max(
                            "silver_ingestion_time"
                        )
                    )
                    .first()[0]
                )
            )

            return


        # ----------------------------------
        # FINAL SAFETY:
        # One source row per employee
        # ----------------------------------

        merge_window = (
            Window
            .partitionBy(
                "employee_id"
            )
            .orderBy(
                F.col("updated_at")
                .desc()
            )
        )


        df_changes = (

            df_changes

            .withColumn(
                "rn",
                F.row_number()
                .over(merge_window)
            )

            .filter(
                F.col("rn") == 1
            )

            .drop("rn")

        )


        # ----------------------------------
        # Expire old rows
        # ----------------------------------

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


        # ----------------------------------
        # Insert new versions
        # ----------------------------------

        (
            df_changes

            .write

            .format("delta")

            .mode("append")

            .saveAsTable(
                gold_table
            )
        )


    # --------------------------------------
    # Update watermark
    # --------------------------------------

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
        "gold_employee",
        "silver.silver_employee",
        "gold.dim_employee",
        new_watermark
    )