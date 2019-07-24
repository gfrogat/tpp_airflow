from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row

import numpy as np

spark = (
    SparkSession.builder.appName("Process ChEMBL25 Assays")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

_data_root = "/local00/bioinf/tpp"

# merged_assays = spark.read.parquet(_data_root + "/merged_assays.parquet")
merged_assays = spark.read.parquet(
    _data_root + "/chembl_25/chembl_25_assays_formatted.parquet"
)

merged_assays = merged_assays.groupby("inchikey").agg(
    F.collect_set("inchi").alias("inchi"),
    F.collect_list("activity").alias("activity"),
    F.collect_list("global_id").alias("index"),
)


schema_labels = T.StructType(
    [
        T.StructField("activity", T.ArrayType(T.IntegerType()), False),
        T.StructField("index", T.ArrayType(T.IntegerType()), False),
    ]
)


@F.udf(returnType=schema_labels)
def sort_labels(activity, index):
    index_order = np.argsort(index)
    activity_sorted = np.array(activity)[index_order].tolist()
    index_sorted = np.array(index)[index_order].tolist()
    row = Row(activity=activity_sorted, index=index_sorted)
    return row


merged_assays = merged_assays.withColumn(
    "labels", sort_labels(F.col("activity"), F.col("index"))
)
merged_assays = merged_assays.select("inchikey", "inchi", "labels.*")

merged_assays.write.parquet(
    _data_root + "/chembl_25/chembl_25_assays_flattened.parquet"
)
