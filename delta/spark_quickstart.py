#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from datetime import datetime

import pyspark
from delta import *
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import functions
from pyspark.sql import types


builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


def create_df_first_data() -> pyspark.sql.DataFrame:
    return spark.createDataFrame(
        [
            (1, datetime(2024, 1, 27, 12, 30, 0)),
            (2, datetime(2024, 1, 27, 12, 40, 0)),
        ],
        types.StructType(  # Define the whole schema within a StructType
            [
                types.StructField("id", types.IntegerType(), False),
                types.StructField("timestamp", types.TimestampType(), False),
            ]
        ),
    )


def create_df_second_data() -> pyspark.sql.DataFrame:
    return spark.createDataFrame(
        [
            (3, datetime(2024, 1, 27, 13, 30, 0)),
            (4, datetime(2024, 1, 27, 13, 40, 0)),
        ],
        types.StructType(  # Define the whole schema within a StructType
            [
                types.StructField("id", types.IntegerType(), False),
                types.StructField("timestamp", types.TimestampType(), False),
            ]
        ),
    )


def create_df_third_data() -> pyspark.sql.DataFrame:
    return spark.createDataFrame(
        [
            (5, datetime(2024, 1, 27, 13, 30, 0)),
            (6, datetime(2024, 1, 27, 13, 40, 0)),
        ],
        types.StructType(  # Define the whole schema within a StructType
            [
                types.StructField("id", types.IntegerType(), False),
                types.StructField("timestamp", types.TimestampType(), False),
            ]
        ),
    )


target_delta_table = "./tmp/spark-delta-table"


# Clear any previous runs
# hutil.rmtree(target_delta_table, ignore_errors=True)

# df_first_data.repartition(1).write.partitionBy("__watermark").format("delta").save(target_delta_table)
#
# df_second_data = spark.createDataFrame([
#    (3, "2024-01-27 11:40:00"),  # Add your data here
#    (4, "2024-01-27 11:40:00"),
# ],
#    types.StructType(  # Define the whole schema within a StructType
#        [
#            types.StructField("id", types.IntegerType(), True),
#            types.StructField("__watermark", types.StringType(), False),
#        ]
#    ), )
# df_second_data.repartition(1).write.mode("append").partitionBy("__watermark").format("delta").save(target_delta_table)
## Read the table
# print("############ Reading the table ###############")
# df = spark.read.format("delta").load(target_delta_table)
# df.show()
# deltaTable = DeltaTable.forPath(spark, target_delta_table)
# deltaTable.history().show(truncate=False)


def get_latest_version_from_delta_table(
    spark: pyspark.sql.SparkSession, target_delta_table: str, debug: bool = False
):
    try:
        delta_table = DeltaTable.forPath(spark, target_delta_table)
        if debug:
            delta_table.history().show(truncate=False)
        version = delta_table.history().agg(functions.max("version")).collect()[0][0]
    except AnalysisException:
        version = None

    return version


def insert_new_data_into_delta_table(
    df_new_data: pyspark.sql.DataFrame,
    spark: pyspark.sql.SparkSession,
    target_delta_table: str,
    write_mode: str = "append",
):
    latest_version = get_latest_version_from_delta_table(spark, target_delta_table)

    if latest_version is None:
        latest_version = 0
    elif latest_version >= 0:
        latest_version += 1

    print("latest_version:", latest_version)
    df_with_version = df_new_data.withColumn("version", functions.lit(latest_version))
    df_with_version.show(truncate=False)
    df_with_version.repartition(1).write.mode(write_mode).partitionBy("version").format(
        "delta"
    ).save(target_delta_table)


def read_latest_snapshot(
    spark: pyspark.sql.SparkSession, target_delta_table: str
) -> pyspark.sql.DataFrame:
    # deltaTable = DeltaTable.forPath(spark, target_delta_table)
    # max_version = deltaTable.history().agg(functions.max("__watermaerk")).collect()[0][0]
    # spark.read.format("delta").option("versionAsOf", max_version)
    filter_column = "__watermark"

    max_watermark = (
        spark.read.format("delta")
        .load(target_delta_table)
        .agg(functions.max(filter_column))
        .collect()[0][0]
    )

    return (
        spark.read.format("delta")
        .load(target_delta_table)
        .where(functions.col(filter_column) == functions.lit(max_watermark))
    )


# df_latest_snapshot = read_latest_snapshot(spark, target_delta_table)
# df_latest_snapshot.show(truncate=False)


def read_all_data(
    spark: pyspark.sql.SparkSession, target_delta_table: str
) -> pyspark.sql.DataFrame:
    return spark.read.format("delta").load(target_delta_table)


# shutil.rmtree(target_delta_table, ignore_errors=True)
#insert_new_data_into_delta_table(create_df_first_data(), spark, target_delta_table)
#insert_new_data_into_delta_table(create_df_second_data(), spark, target_delta_table)
#insert_new_data_into_delta_table(create_df_third_data(), spark, target_delta_table)
#insert_new_data_into_delta_table(create_df_first_data(), spark, target_delta_table,"overwrite")
deltaTable = DeltaTable.forPath(spark, target_delta_table)
#deltaTable.delete(functions.col("version") == functions.lit(get_latest_version_from_delta_table(spark, target_delta_table, True)))
#deltaTable.restoreToVersion(2)
#df = spark.read.format("delta").option("versionAsOf", 3).load(target_delta_table)
#df.show(truncate=False)
#deltaTable.vacuum(0)
#read_all_data(spark, target_delta_table).show(truncate=False)
#print(get_latest_version_from_delta_table(spark, target_delta_table, True))

# pyspark --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
#polars_delta_table ="./tmp/polars-delta-table"
#deltaTable = DeltaTable.forPath(spark, polars_delta_table)
#deltaTable.history().show(truncate=False)
#read_all_data(spark,polars_delta_table).show(truncate=False)
## Upsert (merge) new data
# print("########### Upsert new data #############")
# newData = spark.range(0, 20)
#
# deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")
#
# deltaTable.alias("oldData")\
#    .merge(
#    newData.alias("newData"),
#    "oldData.id = newData.id")\
#    .whenMatchedUpdate(set={"id": col("newData.id")})\
#    .whenNotMatchedInsert(values={"id": col("newData.id")})\
#    .execute()
#
# deltaTable.toDF().show()
#
## Update table data
# print("########## Overwrite the table ###########")
# data = spark.range(5, 10)
# data.write.format("delta").mode("overwrite").save("/tmp/delta-table")
# deltaTable.toDF().show()
#
# deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")
#
## Update every even value by adding 100 to it
# print("########### Update to the table(add 100 to every even value) ##############")
# deltaTable.update(
#    condition=expr("id % 2 == 0"),
#    set={"id": expr("id + 100")})
#
# deltaTable.toDF().show()
#
## Delete every even value
# print("######### Delete every even value ##############")
# deltaTable.delete(condition=expr("id % 2 == 0"))
# deltaTable.toDF().show()
#
## Read old version of data using time travel
# print("######## Read old data using time travel ############")
# df = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
# df.show()
#
## cleanup
# shutil.rmtree("/tmp/delta-table")
#
