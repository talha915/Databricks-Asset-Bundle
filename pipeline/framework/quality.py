from framework.utils import get_spark
from datetime import datetime


spark = get_spark()



def save_quality_result(
    run_id,
    layer,
    table_name,
    check_name,
    status,
    failed_records=0,
    message=None
):

    data = [
        (
            run_id,
            layer,
            table_name,
            check_name,
            status,
            failed_records,
            message,
            datetime.now()
        )
    ]


    columns = [
        "run_id",
        "layer",
        "table_name",
        "check_name",
        "status",
        "failed_records",
        "message",
        "created_time"
    ]


    df = spark.createDataFrame(
        data,
        columns
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
    run_id,
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
            run_id,
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


    else:

        save_quality_result(
            run_id,
            layer,
            table_name,
            "schema_check",
            "PASS"
        )


def check_nulls(
    df,
    run_id,
    layer,
    table_name,
    column
):

    count = (
        df.filter(
            df[column].isNull()
        )
        .count()
    )


    status = (
        "PASS"
        if count == 0
        else "FAIL"
    )


    save_quality_result(
        run_id,
        layer,
        table_name,
        f"{column}_null_check",
        status,
        count
    )