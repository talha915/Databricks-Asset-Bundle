from framework.utils import get_spark
from framework.config import CATALOG, BRONZE_DB, SILVER_DB
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

spark = get_spark()

def transform(
    df: DataFrame
)-> DataFrame:
    return (
        df
        .withColumn("silver_ingestion_time", F.current_timestamp())
        .select(
            F.col("Department_id").cast("int").alias("department_id"),
            F.col("Name").cast("string").alias("department_name"),
            F.col("file_path").cast("string").alias("file_path"),
            F.col("ingestion_time").cast("timestamp").alias("bronze_ingestion_time"),
            "silver_ingestion_time"
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
            "bronze_ingestion_time",
            "silver_ingestion_time"
        )
    )
    
    return df_dedup