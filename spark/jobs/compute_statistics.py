from pyspark.sql import functions as F
from pyspark.sql import types as T


@F.udf(returnType=T.IntegerType())
def get_max(values):
    if len(values) > 0:
        max_value = max(values)
    else:
        max_value = 0

    return max_value


df = spark.read.parquet("/data/tmp/maccs_rdkit.parquet/")
df.withColumn("max_value", get_max(F.col("index"))).agg({"max_value": "max"}).show()

df.withColumn("max_value", get_max(F.col("maccs_fp"))).agg({"max_value": "max"}).show()

df.withColumn("max_value", get_max(F.col("rdkit_fp"))).agg({"max_value": "max"}).show()
