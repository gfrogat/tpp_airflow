from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from pathlib import Path

spark = (
    SparkSession.builder.appName("Process ZINC15 Assays")
        .config("spark.sql.execution.arrow.enabled", "true")
        .getOrCreate()
)

_data_root = Path("/local00/bioinf/tpp")

df = spark.read.parquet((_data_root / "ZINC15/zinc15_data_full.parquet").as_posix())
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

triples = df.groupby(["mol_id", "gene_name"]).agg(
    F.mean("affinity").alias("avg_affinity"), F.collect_set("mol_file").alias("mol_file"))

target_size = df.select("gene_name").distinct().count()
compound_size = df.select("mol_id").distinct().count()

# necessary?
assert df.select("gene_name").distinct().intersect(triples.select("gene_name").distinct()).count() == target_size
assert df.select("mol_id").distinct().intersect(triples.select("mol_id").distinct()).count() == compound_size

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
        .select(["mol_id", "mol_file", "gene_name", "activity"])
)

"""
triples = triples.withColumn("mol_file_size", F.size("mol_file")).filter(F.col("mol_file_size") == 1).select("mol_id",
                                                                                                       "mol_file",
                                                                                                       "gene_name",
                                                                                                       "activity")
"""

triples.write.parquet((_data_root / "ZINC15/zinc15_data_cleaned.parquet").as_posix())
