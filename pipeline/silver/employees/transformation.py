from framework.utils import get_spark
from framework.config import CATALOG, BRONZE_DB, SILVER_DB
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

spark = get_spark()


def get_unprocessed_bronze_df(
    df_a: DataFrame,
    df_b: DataFrame,
    df_c: DataFrame,
    max_ingestion_time
)-> DataFrame:
    """
    Filter unprocessed bronze rows
    """
    unprocessed_df_emp_a = df_a.filter(F.col("ingestion_time") > max_ingestion_time)
    unprocessed_df_emp_b = df_b.filter(F.col("ingestion_time") > max_ingestion_time)
    unprocessed_df_emp_c = df_c.filter(F.col("ingestion_time") > max_ingestion_time)

    return unprocessed_df_emp_a, unprocessed_df_emp_b, unprocessed_df_emp_c



def transform_json_df(
    df: DataFrame
) -> DataFrame:
    """
    Source of tis dataframe was nested json
    -> Parsing nested json and selecting required columns
    """
    return (
        df
        .withColumn("First_Name", F.get_json_object("customer", "$.First_Name"))
        .withColumn("Last_Name", F.get_json_object("customer", "$.Last_Name"))
        .select(
            "Employee_id",
            "First_Name",
            "Last_Name",
            "Gender",
            "Salary",
            "Date_of_Birth",
            "Age",
            "Country",
            "Department_id",
            "Date_of_Joining",
            "Manager_id",
            "Currency",
            "End_Date",
            "file_path",
            "ingestion_time"
        )
    )
    

def transform_xml_df(
    df: DataFrame
) -> DataFrame:
    """
    Source of tis dataframe was xml
    -> Parsing xml column and selecting required columns
    """
    return(
        df
        .withColumn("First_Name", F.regexp_extract("customer", r"<First_Name>(.*?)</First_Name>", 1))
        .withColumn("Last_Name",F.regexp_extract("customer", r"<Last_Name>(.*?)</Last_Name>", 1))
        .select(
            "Employee_id",
            "First_Name",
            "Last_Name",
            "Gender",
            "Salary",
            "Date_of_Birth",
            "Age",
            "Country",
            "Department_id",
            "Date_of_Joining",
            "Manager_id",
            "Currency",
            "End_Date",
            "file_path",
            "ingestion_time"
        )
    )


def transform(
    df_a: DataFrame,
    df_b: DataFrame,
    df_c: DataFrame
)-> DataFrame:
    
    df_a = df_a.drop("_rescued_data")
    df_trans_b = transform_json_df(df_b)
    df_trans_c = transform_xml_df(df_c)

    df_union = df_a.union(df_trans_b).union(df_trans_c)

    return (
        df_union
        .withColumn("Date_of_Birth", F.to_date("Date_of_Birth", "dd/MM/yyyy"))
        .withColumn("Date_of_Joining", F.to_date("Date_of_Joining", "dd/MM/yyyy"))
        .withColumn("End_Date", F.to_date("End_Date", "dd/MM/yyyy"))
        .select(
            F.col("Employee_id").cast("int").alias("employee_id"),
            F.col("First_Name").cast("string").alias("first_name"),
            F.col("Last_Name").cast("string").alias("last_name"),
            F.col("Gender").cast("string").alias("gender"),
            F.col("Salary").cast("double").alias("salary"),
            F.col("Date_of_Birth").cast("date").alias("date_of_birth"),
            F.col("Age").cast("int").alias("age"),
            F.col("Country").cast("string").alias("country"),
            F.col("Department_id").cast("int").alias("department_id"),
            F.col("Date_of_Joining").cast("date").alias("date_of_joining"),
            F.col("Manager_id").cast("int").alias("manager_id"),
            F.col("Currency").cast("string").alias("currency"),
            F.col("End_Date").cast("date").alias("end_date"),
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
            on=["department_id", "employee_id"],
            how="left_anti"
        )
        .select(
            "employee_id",
            "first_name",
            "last_name",
            "gender",
            "salary",
            "date_of_birth",
            "age",
            "country",
            "department_id",
            "date_of_joining",
            "manager_id",
            "currency",
            "end_date",
            "file_path",
            "ingestion_time"
        )
        .withColumnRenamed("ingestion_time", "bronze_ingestion_time")
    )
    
    return df_dedup