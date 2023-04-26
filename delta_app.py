import pyspark
from delta import *
from pyspark.sql import SparkSession


def create_spark_session():
    return (
        SparkSession.builder.appName("myApp")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def create_debug_spark_session():
    builder = (
        SparkSession.builder.appName("myApp")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


spark = create_debug_spark_session()

DeltaTable.createIfNotExists(sparkSession=spark).tableName(
    "default.people10m"
).addColumn("id", "INT").addColumn("firstName", "STRING").addColumn(
    "middleName", "STRING"
).addColumn(
    "lastName", "STRING", comment="surname"
).addColumn(
    "gender", "STRING"
).addColumn(
    "birthDate", "TIMESTAMP"
).addColumn(
    "dateOfBirth",
    pyspark.sql.types.DateType(),
    generatedAlwaysAs="CAST(birthDate AS DATE)",
).addColumn(
    "ssn", "STRING"
).addColumn(
    "salary", "INT"
).addColumn(
    "__watermark", "STRING"
).partitionedBy(
    "__watermark"
).execute()

df = spark.read.format("delta").load("./delta-table")
df.show()
