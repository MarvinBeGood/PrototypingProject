from deltalake import Field
from deltalake.writer import write_deltalake
import polars
from datetime import datetime
from pydantic import BaseModel, ConfigDict
import pathlib
from deltalake.data_catalog import DataCatalog
from deltalake.table import DeltaTable
from deltalake.schema import Schema, PrimitiveType

timestamp = datetime.now()
watermark = timestamp.strftime(format="%Y-%m-%d: %H:%M:%S.%f")

#df = polars.DataFrame(
#    {
#        "row_id": [1, 2, 3],
#        "any_double": [1.2, 124.2, 19.02],
#        "some_timestamp": [timestamp, timestamp, timestamp],
#        "__watermark": [watermark, watermark, watermark],
#    }
#)
df = polars.DataFrame(
    {
        "row_id": [4, 5, 6],
        "any_double": [2.0, 11.0, 20.0],
        "some_timestamp": [timestamp, timestamp, timestamp],
        "__watermark": [watermark, watermark, watermark],
    }
)

polars_table_name = "polars-delta-table"
polars_table_path = rf"./tmp/{polars_table_name}"
polars_table_description = "This Delta table is created with polars"

#write_deltalake(
#   table_or_uri=polars_table_path,
#   data=df.to_pandas(),
#   schema=Schema(
#       [
#           Field("row_id", "integer"),
#           Field("any_double", "double"),
#           Field("some_timestamp", "timestamp"),
#           Field("__watermark", "string"),
#       ]
#   ),
#   description=polars_table_description,
#   mode="append",
#   name=polars_table_name,
#   engine="pyarrow",
#   partition_by=["__watermark"],
#)
polars_delta_table = DeltaTable(polars_table_path)
#for value in polars_delta_table.history():
#    print(value)

#print(polars_delta_table.schema())

df_polars_delta_table = polars.read_delta(r"./tmp/polars-delta-table",version=1)
print(df_polars_delta_table.head(10))

#df_spark_delta_table = polars.read_delta(r"./tmp/spark-delta-table")
#
#print(df_spark_delta_table.head(100))


# Note that this function does NOT register this table in a data catalog.

# A locking mechanism is needed to prevent unsafe concurrent writes to a delta lake directory when writing to S3. DynamoDB is the only available locking provider at the moment in delta-rs. To enable DynamoDB as the locking provider, you need to set the AWS_S3_LOCKING_PROVIDER to 'dynamodb' as a storage_option or as an environment variable.
# Additionally, you must create a DynamoDB table with the name 'delta_rs_lock_table' so that it can be automatically discovered by delta-rs. Alternatively, you can use a table name of your choice, but you must set the DYNAMO_LOCK_TABLE_NAME variable to match your chosen table name. The required schema for the DynamoDB table is as follows:
# Key Schema: AttributeName=key, KeyType=HASH
# Attribute Definitions: AttributeName=key, AttributeType=S

# Please note that this locking mechanism is not compatible with any other locking mechanisms, including the one used by Spark
# pyarrow_options={"partitions": [("year", "=", "2021")]},
# polars.read_delta(table_path)
