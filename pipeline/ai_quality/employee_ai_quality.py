from framework.ai_quality import run_ai_quality_check
from framework.utils import get_spark
import pyspark.sql.functions as F


spark = get_spark()


def run():

    df = spark.table(
        "ai_lab_demo.silver.silver_employee"
    )


    salary_candidates = (
        df
        .filter(
            F.col("salary") > 149975
        )
    )


    run_ai_quality_check(

        salary_candidates,

        "silver_employee",

        "salary_anomaly",

        [
            "employee_id",
            "first_name",
            "last_name",
            "country",
            "currency",
            "salary",
            "department_id"
        ],


        """
        Analyze this employee salary.

        Employee:
        {first_name} {last_name}

        Country:
        {country}

        Currency:
        {currency}

        Salary:
        {salary}

        Department:
        {department_id}

        Identify:
        - Is this abnormal?
        - Possible reason?
        - Recommended action?
        """

    )