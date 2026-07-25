from framework.utils import get_spark
from framework.config import CATALOG, SILVER_DB
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


silver_table = (
    f"{CATALOG}.{SILVER_DB}.silver_department"
)


def test_silver_table_exists(spark):

    assert spark.catalog.tableExists(
        silver_table
    )


def test_silver_has_data(spark):

    df = spark.table(
        silver_table
    )

    assert df.count() > 0


def test_silver_columns(spark):

    df = spark.table(
        silver_table
    )

    columns = df.columns

    assert "department_id" in columns
    assert "department_name" in columns



def test_silver_no_duplicate_department(spark):

    df = spark.table(
        silver_table
    )

    duplicate_count = (
        df.groupBy("department_id")
        .count()
        .filter("count > 1")
        .count()
    )

    assert duplicate_count == 0