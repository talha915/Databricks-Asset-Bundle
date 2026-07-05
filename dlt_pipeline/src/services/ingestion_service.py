from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
class IngestionService:

    # def __init__(self, spark: SparkSession):
    #     self.spark = spark

    def read_csv_autoloader(path, schema_location):
        return (
            spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("header", "true") \
            .option(
                "cloudFiles.schemaLocation",
                schema_location
            ).load(path)
        )