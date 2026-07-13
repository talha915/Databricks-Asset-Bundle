from pyspark.sql import SparkSession


def get_spark():

    return SparkSession.builder \
        .appName("CustomerLakehouse") \
        .getOrCreate()



def save_delta(df,database,table):

    df.write \
      .format("delta") \
      .mode("overwrite") \
      .saveAsTable(
          f"{database}.{table}"
      )