from datetime import datetime


def create_metric(
    job,
    status,
    records,
    error=None
):

    return {

        "job_name":job,

        "run_time":datetime.now(),

        "status":status,

        "records_processed":records,

        "error":error
    }