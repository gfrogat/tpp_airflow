from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row

import numpy as np
from pathlib import Path

spark = (
    SparkSession.builder.appName("Process ChEMBL25 Assays")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

_data_root = Path("/local00/bioinf/tpp")

# merged_assays = spark.read.parquet(_data_root + "/merged_assays.parquet")
merged_assays = spark.read.parquet(
    (_data_root / "chembl_25/chembl_25_assays_formatted.parquet").as_posix()
)

flattened_assays = merged_assays.groupby("inchikey").agg(
    F.collect_set("mol_file").alias("mol_file"),
    F.sort_array(F.collect_list(F.struct(F.col("global_id"), F.col("activity")))).alias(
        "activity_tuples"
    ),
)


schema_labels = T.StructType(
    [
        T.StructField("index", T.ArrayType(T.IntegerType()), False),
        T.StructField("activity", T.ArrayType(T.IntegerType()), False),
    ]
)


@F.udf(returnType=schema_labels)
def sort_labels(activity_tuples):
    index = [row[0] for row in activity_tuples]
    activity = [row[1] for row in activity_tuples]
    row = Row(index=index, activity=activity)
    return row


result_assays = flattened_assays.withColumn(
    "labels", sort_labels(F.col("activity_tuples"))
).select("inchikey", "mol_file", "labels.*")

result_assays.write.parquet(
    (_data_root / "chembl_25/chembl_25_assays_flattened.parquet").as_posix()
)
