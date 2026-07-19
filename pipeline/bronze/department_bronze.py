from framework.config import CATALOG, BRONZE_DB, departments


def run():

    df = spark.read \
        .format("csv") \
        .option("header","true") \
        .load(departments)


    count=df.count()


    df.write \
      .format("delta") \
      .mode("append") \
      .saveAsTable(
          f"{CATALOG}.{BRONZE_DB}.department"
      )


    return count