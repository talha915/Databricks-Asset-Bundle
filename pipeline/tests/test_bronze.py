import pytest
from pyspark.sql import SparkSession
from framework.config import CATALOG, BRONZE_DB

@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


@pytest.fixture(scope="session")
def table_names(spark):
    tables = spark.catalog.listTables(f"{CATALOG}.{BRONZE_DB}")

    return [
        f"{CATALOG}.{BRONZE_DB}.{table.name}"
        for table in tables
    ]


def test_tables_exist(table_names):
    assert len(table_names) > 0


def test_bronze_table_has_data(spark, table_names):
    for table in table_names:
        df = spark.read.table(table)
        assert not df.isEmpty(), f"Table {table} is empty"


def test_bronze_required_columns(spark, table_names):
    for table in table_names:
        df = spark.read.table(table)
        assert "file_path" in df.columns
        assert "ingestion_time" in df.columns