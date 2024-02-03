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
import os
from typing import Any
import pydantic
import pyspark
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import functions
from pyspark.sql import types
from pyspark.sql.functions import col, lit, when, max

from pydantic import BaseModel, ConfigDict
import pathlib

os.environ[
    "PYSPARK_PYTHON"
] = r"D:\Users\Non-BKU\Git_repos\PrototypingProject\.venv\Scripts\python.exe"
os.environ["HADOOP_HOME"] = r"D:\Users\Non-BKU\Tools\hadoop-3.3.0"

from abc import ABC, abstractmethod


class PipelineABC(ABC):
    @abstractmethod
    def execute_pipeline(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        This method must be implemented by the user.
        """
        pass


class HudiSparkConfig:
    def __init__(
            self,
            app_name: str,
            spark_version: str = "3.4",
            hudi_bundle_version: str = "2.12:0.14.1",
    ):
        self.spark_version = spark_version
        self.hudi_bundle_version = hudi_bundle_version
        self.spark_session = (
            pyspark.sql.SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config(
                "spark.jars.packages",
                f"org.apache.hudi:hudi-spark{spark_version}-bundle_{hudi_bundle_version}",
            )
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config(
                "spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
            )
            .config(
                "spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar"
            )
        ).getOrCreate()


class HudiTable(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    app_name: str
    table_name: str
    base_path: pathlib.Path
    record_key_field_name: str
    partition_path_field_name: str
    pre_combine_field_name: str
    hudi_spark_config: HudiSparkConfig
    __hudi_options: dict
    __table_path: pathlib.Path
    __table_path_as_string: str

    def __init__(self, **data: Any):
        super().__init__(**data)
        self.__hudi_options: dict = {
            "hoodie.table.name": self.table_name,
            "hoodie.metadata.enable": True,
            "hoodie.datasource.write.recordkey.field": self.record_key_field_name,
            "hoodie.datasource.write.table.name": self.table_name,
            "hoodie.datasource.write.partitionpath.field": self.partition_path_field_name,
            "hoodie.datasource.write.precombine.field": self.pre_combine_field_name,
            # 'hoodie.upsert.shuffle.parallelism': 2,
            # 'hoodie.insert.shuffle.parallelism': 2
            "hoodie.schema.on.read.enable": True,
        }
        self.__table_path = self.base_path.joinpath(self.table_name)
        self.__table_path_as_string = str(self.__table_path.absolute())

    def create_df_data(
            self, rows: list, schema: types.StructType
    ) -> pyspark.sql.DataFrame:
        return self.hudi_spark_config.spark_session.createDataFrame(
            rows,
            schema,
        )

    def append_to_hudi_table(self, df: pyspark.sql.DataFrame) -> None:
        df.write.format("hudi").options(**self.__hudi_options).mode("append").save(
            self.__table_path_as_string
        )

    def overwrite_hudi_table(self, df: pyspark.sql.DataFrame) -> None:
        """
        Deletes the Hudi Table and writes the new data into the hudi table
        :param df:
        :return:
        """
        df.write.format("hudi").options(**self.__hudi_options).mode("overwrite").save(
            self.__table_path_as_string
        )

    def update_hudi_table(self, pipeline: PipelineABC):
        update_df = pipeline.execute_pipeline(df=self.read_all_data_from_hudi_table())

        update_df.write.format("hudi").options(**self.__hudi_options).mode(
            "append"
        ).save(self.__table_path_as_string)

    def delete_rows_from_hudi_table(self, pipeline: PipelineABC):
        self.__hudi_options["hoodie.datasource.write.operation"] = "delete"

        delete_df = pipeline.execute_pipeline(df=self.read_all_data_from_hudi_table())

        delete_df.write.format("hudi").options(**self.__hudi_options).mode(
            "append"
        ).save(self.__table_path_as_string)

    def time_travel_in_history(self, timestamp: str) -> pyspark.sql.DataFrame:
        # micro_secs = timestamp.strftime(format="%f")[:3]

        return self.hudi_spark_config.spark_session.read.format("hudi").option(
            "as.of.instant", timestamp  # .strftime(format="%Y-%m-%d: %H:%M:%S.") + micro_secs
        ).load(self.__table_path_as_string)

    def read_all_data_from_hudi_table(self) -> pyspark.sql.DataFrame:
        return self.hudi_spark_config.spark_session.read.format("hudi").load(
            self.__table_path_as_string
        )

    def read_meta_data_from_hudi_table(self) -> pyspark.sql.DataFrame:
        return self.hudi_spark_config.spark_session.read.format("hudi").load(
            str(self.__table_path.joinpath(".hoodie").joinpath("metadata").absolute()))


def create_watermark_string() -> str:
    return (
        datetime.now()
        .isoformat()
        .replace("-", "")
        .replace("T", "")
        .replace(":", "")
        .replace(".", "")
    )


hudi_spark_config = HudiSparkConfig("myhudiManager")

hudi_table = HudiTable(
    app_name="myHudiApp",
    base_path=pathlib.Path(r"D:\Users\Non-BKU\Git_repos\PrototypingProject\hudi\tmp"),
    table_name="hudi-table",
    record_key_field_name="row_id",
    partition_path_field_name="__watermark",
    pre_combine_field_name="__watermark",
    hudi_spark_config=hudi_spark_config,
)


class UpdatePipeline(PipelineABC):
    def execute_pipeline(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        # schema check schlägt NICHT fehlt weil die spalte test nullable ist
        return df.where(col("row_id") == lit(2)).withColumn("row_id", lit(4))


class UpdatePipeline2(PipelineABC):
    def execute_pipeline(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        # schema check schlägt fehlt weil die spalte test2 nicht nullable ist
        return df.withColumn(
            "test2", when(col("row_id") == lit(2), "Yes").otherwise("No")
        )


class DeletePipeline(PipelineABC):
    def execute_pipeline(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return df.filter(col("row_id") == lit(4))


class DeletePipelineWhereMaxWatermark(PipelineABC):
    def execute_pipeline(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return df.filter(
            col("__watermark") == lit(df.agg(max("__watermark")).collect()[0][0])
        )


# hudi_table.update_hudi_table(Pipeline2())
watermark = create_watermark_string()

# hudi_table.append_to_hudi_table(
#  hudi_table.create_df_data(
#      schema=types.StructType(  # Define the whole schema within a StructType
#          [
#              types.StructField("row_id", types.IntegerType(), False),
#              types.StructField("timestamp", types.TimestampType(), False),
#              types.StructField("__watermark", types.StringType(), False),
#              types.StructField("test", types.StringType(), True),
#          ]
#      ),
#      rows=[
#          (3, datetime(2024, 1, 27, 12, 30, 0), watermark,None),
#          (4, datetime(2024, 1, 27, 12, 40, 0), watermark,None),
#      ],
#  )
# )
# hudi_table.delete_rows_from_hudi_table(DeletePipeline())
# hudi_table.delete_rows_from_hudi_table(DeletePipelineWhereMaxWatermark())

hudi_table.time_travel_in_history("20240201145807505").show(truncate=False)
hudi_table.read_all_data_from_hudi_table().pandas_api().orderBy("__watermark").show(truncate=False)

# shutil.rmtree(target_delta_table, ignore_errors=True)
