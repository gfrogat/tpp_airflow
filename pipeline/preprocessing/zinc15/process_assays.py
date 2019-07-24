from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("Process ZINC15 Assays")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

df = spark.read.csv("/data/ZINC15/activities_smi.csv", header=True)
df = df.dropna()

"""
# needed?
w = Window.partitionBy("zinc_id")

df = df.withColumn(
    "count",
    F.count("smiles").over(w)
).withColumn(
    "rank",
    F.row_number().over(w.orderBy(F.desc("count")))
).filter(F.col("rank") == 1)
"""

triples = (
    df.select(["zinc_id", "gene_name", "affinity"])
    .groupby(["zinc_id", "gene_name"])
    .agg(F.mean("affinity").alias("avg_affinity"))
)

target_size = df.select("gene_name").distinct().count()
compound_size = df.select("zinc_id").distinct().count()

# necessary?
# assert df.select("gene_name").distinct().intersect(triples.select("gene_name").distinct()).count() == target_size
# assert df.select("zinc_id").distinct().intersect(triples.select("zinc_id").distinct()).count() == compound_size

triples = triples.withColumn(
    "activity", F.when(F.col("avg_affinity") > 8, 1).otherwise(-1)
)

w = Window.partitionBy("gene_name")

triples = (
    triples.withColumn("actives", F.count(F.when(F.col("activity") == 1, 0)).over(w))
    .withColumn("inactives", F.count(F.when(F.col("activity") == -1, 0)).over(w))
    .filter(
        (F.col("actives") > 10)
        & (F.col("inactives") > 10)
        & (F.col("actives") + F.col("inactives") > 25)
    )
    .select(["zinc_id", "gene_name", "activity"])
)

triples.write.parquet("/data/ZINC15/zinc15_assays.parquet")
