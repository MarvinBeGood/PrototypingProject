import pyspark
from delta import *

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
data = spark.range(0, 5)
data.coalesce(1).write.mode("overwrite").format("delta").save("./delta-table")

df = spark.read.format("delta").load("./delta-table")
df.show()
