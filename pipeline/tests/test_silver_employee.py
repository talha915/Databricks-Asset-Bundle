from framework.utils import get_spark
from framework.config import CATALOG, SILVER_DB
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


silver_table = (
    f"{CATALOG}.{SILVER_DB}.silver_employee"
)


def test_employee_silver_table_exists(spark):

    assert spark.catalog.tableExists(
        silver_table
    )


def test_employee_silver_has_data(spark):

    df = spark.table(
        silver_table
    )

    assert df.count() > 0



def test_employee_silver_columns(spark):

    df = spark.table(
        silver_table
    )

    columns = df.columns

    assert "employee_id" in columns



def test_employee_silver_no_duplicates(spark):

    df = spark.table(
        silver_table
    )

    duplicate_count = (
        df.groupBy("employee_id", "department_id", "first_name", "manager_id")
        .count()
        .filter("count > 1")
        .count()
    )

    assert duplicate_count == 0