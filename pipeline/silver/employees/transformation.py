from framework.utils import get_spark
from framework.config import CATALOG, BRONZE_DB, SILVER_DB
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

spark = get_spark()


def get_unprocessed_bronze_df(
    df_a: DataFrame,
    df_b: DafaFrame,
    df_c: DataFrame,
    max_ingestion_time: F.timestamp()
)-> DataFrame:
    """
    Filter unprocessed bronze rows
    """
    unprocessed_df_emp_a = df_a.filter(F.col("ingestion_time") > max_ingestion_time)
    unprocessed_df_emp_b = df_b.filter(F.col("ingestion_time") > max_ingestion_time)
    unprocessed_df_emp_c = df_c.filter(F.col("ingestion_time") > max_ingestion_time)

    return unprocessed_df_emp_a, unprocessed_df_emp_b, unprocessed_df_emp_c



def transform(
    df_a: DataFrame,
    df_b: DataFrame,
    df_c: DataFrame
)-> DataFrame:
    
    df_trans_b = transform_json_df(df_b)
    df_trans_c = transform_xml_df(df_c)

    return (
        df
        .select(
            F.col("Department_id").cast("int").alias("department_id"),
            F.col("Name").cast("string").alias("department_name"),
            F.col("file_path").cast("string").alias("file_path"),
            F.col("ingestion_time").cast("timestamp").alias("ingestion_time")
        )
    )


def deduplicate_df(
    bronze_df: DataFrame,
    silver_df: DataFrame
) -> DataFrame:
    
    df_dedup = (
        bronze_df
        .join(
            silver_df,
            on=["department_id", "department_name"],
            how="left_anti"
        )
        .select(
            "department_id",
            "department_name",
            "file_path",
            "ingestion_time"
        )
        .withColumnRenamed("ingestion_time", "bronze_ingestion_time")
    )
    
    return df_dedup