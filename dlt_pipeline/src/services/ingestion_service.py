from pyspark.sql.functions import F

class IngestionService:

    def read_csv_autoloader(path, schema_location):
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option(
                "cloudFiles.schemaLocation",
                schema_location
            )
            .load(path)
        )