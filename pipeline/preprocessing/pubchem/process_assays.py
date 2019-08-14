from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession.builder.appName("Process PubChem Assays")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

pubchem_assay_schema = T.StructType(
    [
        T.StructField("aid", T.IntegerType(), False),
        T.StructField("cid", T.IntegerType(), False),
        T.StructField("activity_outcome", T.IntegerType(), False),
    ]
)


df = spark.read.parquet("/local00/bioinf/tpp/pubchem_20190717/pubchem.parquet")
df = df.dropna()

# remove assays with few measurements
w = Window.partitionBy("aid")

df = (
    df.select(["aid", "cid", "activity_outcome"])
    .withColumn("count", F.count("*").over(w))
    .filter(F.col("count") > 25)
    .drop("count")
)


df = df.withColumn(
    "affinity", F.when(F.col("activity_outcome") == 3, 1.0).otherwise(0.0)
).drop("activity_outcome")


df = (
    df.groupby(["aid", "cid"])
    .agg(F.mean(F.col("affinity")).alias("avg_affinity"))
    .filter(F.col("avg_affinity") != 0.5)
)

df = df.withColumn(
    "activity", (2 * (F.round(F.col("avg_affinity")) - 0.5)).cast(T.IntegerType())
).drop("avg_affinity")


w = Window.partitionBy("aid")

selected_df = (
    df.withColumn("actives", F.count(F.when(F.col("activity") == 1, 0)).over(w))
    .withColumn("inactives", F.count(F.when(F.col("activity") == -1, 0)).over(w))
    .filter(
        (F.col("actives") > 10)
        & (F.col("inactives") > 10)
        & (F.col("actives") + F.col("inactives") > 25)
    )
    .select(["cid", "aid", "activity"])
)


selected_df.write.parquet(
    "/local00/bioinf/tpp/pubchem_20190717/pubchem_processed.parquet"
)
