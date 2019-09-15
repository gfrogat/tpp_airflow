from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F


def standardize_zinc15(spark: SparkSession, data_path: Path):
    # ZINC15 - load data
    data = spark.read.parquet(data_path.as_posix())

    # Compound IDs
    compound_ids = (
        data.select("inchikey", "mol_id")
        .dropDuplicates()
        .sort(F.asc("mol_id"))
        .withColumn("dataset", F.lit("ZINC15"))
    )

    # Assay IDs
    assay_ids = (
        data.select("gene_name")
        .distinct()
        .sort(F.asc("gene_name"))
        .withColumn("assay_id", F.col("gene_name"))
        .drop("gene_name")
        .withColumn("dataset", F.lit("ZINC15"))
    )

    # Data
    data = (
        data.withColumn("dataset", F.lit("ZINC15"))
        .withColumn("assay_id", F.col("gene_name"))
        .select("inchikey", "assay_id", "mol_file", "activity", "dataset")
    )

    # Filter out non agreeing activity labels due to inchikey collision
    w = Window.partitionBy("inchikey", "assay_id")
    data = (
        data.withColumn("size", F.size(F.collect_set("activity").over(w)))
        .filter(F.col("size") == 1)
        .drop("size")
    )

    return data, compound_ids, assay_ids
