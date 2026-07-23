from pyspark.sql import functions as F
from pyspark.sql.window import Window


def transform(df):

    window = Window.orderBy(
        F.monotonically_increasing_id()
    )


    return (

        df

        .select(
            "department_id",
            "department_name"
        )


        # Detect changes
        .withColumn(
            "hash_value",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("department_name")
                ),
                256
            )
        )


        # Surrogate key
        .withColumn(
            "department_key",
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