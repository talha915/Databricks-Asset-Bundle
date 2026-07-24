from pyspark.sql import functions as F
from pyspark.sql.window import Window


def transform(df):

    window = Window.orderBy(
        F.monotonically_increasing_id()
    )

    return (

        df

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
            "manager_id",
            "currency",
            "date_of_joining"
        )


        # Detect changes
        .withColumn(
            "hash_value",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("first_name"),
                    F.col("last_name"),
                    F.col("gender"),
                    F.col("salary"),
                    F.col("date_of_birth"),
                    F.col("country"),
                    F.col("department_id"),
                    F.col("manager_id"),
                    F.col("currency"),
                    F.col("date_of_joining")
                ),
                256
            )
        )


        # Surrogate key
        .withColumn(
            "employee_key",
            F.row_number()
            .over(window)
        )


        .withColumn(
            "start_date",
            F.current_date()
        )


        .withColumn(
            "end_date",
            F.lit(None)
            .cast("date")
        )


        .withColumn(
            "is_current",
            F.lit(True)
        )


        .withColumn(
            "created_at",
            F.current_timestamp()
        )


        .withColumn(
            "updated_at",
            F.current_timestamp()
        )
    )