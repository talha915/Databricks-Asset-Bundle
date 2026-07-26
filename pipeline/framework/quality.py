from framework.utils import get_spark
from pyspark.sql.window import Window
from datetime import datetime
import pyspark.sql.functions as F


spark = get_spark()



from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)


def save_quality_result(
    layer,
    table_name,
    check_name,
    status,
    failed_records=0,
    message=None
):

    schema = StructType([
        StructField("layer", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("check_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("failed_records", LongType(), True),
        StructField("message", StringType(), True),
        StructField("created_time", TimestampType(), True)
    ])


    data = [
        (
            layer,
            table_name,
            check_name,
            status,
            int(failed_records),
            message,
            datetime.now()
        )
    ]


    df = spark.createDataFrame(
        data,
        schema=schema
    )


    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "ai_lab_demo.system.quality_results"
        )
    )



def check_columns(
    df,
    layer,
    table_name,
    required_columns
):

    missing = [
        c
        for c in required_columns
        if c not in df.columns
    ]


    if missing:

        save_quality_result( 
            layer,
            table_name,
            "schema_check",
            "FAIL",
            0,
            str(missing)
        )

        raise Exception(
            f"Missing columns {missing}"
        )


    save_quality_result(
    
        layer,
        table_name,
        "schema_check",
        "PASS"
    )



def check_nulls(
    df,
    layer,
    table_name,
    column
):

    count = (
        df.filter(
            F.col(column).isNull()
        )
        .count()
    )


    status = (
        "PASS"
        if count == 0
        else "FAIL"
    )


    save_quality_result(
        layer,
        table_name,
        f"{column}_null_check",
        status,
        count
    )


    if count > 0:
        raise Exception(
            f"{column} contains null values"
        )



def check_employee_business_key(
    df,
    layer,
    table_name
):

    window = Window.partitionBy(
        "employee_id",
        "department_id",
        "end_date"
    )


    df_check = (
        df
        .withColumn(
            "duplicate_count",
            F.count("*").over(window)
        )
    )


    rejected_df = (
        df_check
        .filter(
            F.col("duplicate_count") > 1
        )
        .withColumn(
            "reject_reason",
            F.lit(
                "Duplicate employee business key"
            )
        )
        .drop(
            "duplicate_count"
        )
    )


    valid_df = (
        df_check
        .filter(
            F.col("duplicate_count") == 1
        )
        .drop(
            "duplicate_count"
        )
    )


    rejected_count = rejected_df.count()


    save_quality_result(
        layer,
        table_name,
        "employee_business_key_check",
        "PASS" if rejected_count == 0 else "FAIL",
        rejected_count,
        "Duplicate employee_id + manager_id + country move to reject"
    )


    if rejected_count > 0:
        (
            rejected_df
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(
                "ai_lab_demo.system.rejected_rows"
            )
        )


    return valid_df