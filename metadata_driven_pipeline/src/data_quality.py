from pyspark.sql.functions import col


def check_not_null(df,column):

    total=df.count()

    bad=df.filter(
        col(column).isNull()
    ).count()

    return {
        "column":column,
        "total_records":total,
        "failed_records":bad,
        "status": "FAILED" if bad > 0 else "PASSED"
    }


def check_positive(df,column):

    bad=df.filter(
        col(column)<=0
    ).count()

    return {
        "column":column,
        "failed_records":bad,
        "status":
        "FAILED" if bad>0 else "PASSED"
    }
