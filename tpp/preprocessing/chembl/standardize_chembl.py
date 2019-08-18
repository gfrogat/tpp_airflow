from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F


def standardize_chembl(spark: SparkSession, compounds_path: Path, assays_path: Path):
    # ChEMBL - load compounds
    compounds = spark.read.parquet(compounds_path.as_posix())

    # ChEMBL - load assays
    assays = spark.read.parquet(assays_path.as_posix())

    # Reformat activity column
    assays = assays.filter(F.col("activity").isin([1, 3])).withColumn(
        "activity", F.col("activity") - 2
    )

    # Only use assays with more than 100 measurements
    w = Window.partitionBy("assay_id")
    assays = (
        assays.select("assay_id", "mol_id", "activity")
        .withColumn("count", F.count("*").over(w))
        .filter(F.col("count") > 100)
        .drop("count")
        .withColumn("dataset", F.lit("ChEMBL"))
    )

    # Compound IDs
    compound_ids = (
        compounds.select("inchikey", "mol_id")
        .sort(F.asc("mol_id"))
        .withColumn("dataset", F.lit("ChEMBL"))
    )

    # Assay IDs
    assay_ids = (
        assays.select("assay_id")
        .distinct()
        .sort(F.asc("assay_id"))
        .withColumn("dataset", F.lit("ChEMBL"))
    )

    # ChEMBL - Merge Compounds and Assays
    data = assays.alias("a").join(
        compounds.alias("c"), F.col("a.mol_id") == F.col("c.mol_id")
    )

    data = data.withColumn("dataset", F.lit("ChEMBL")).select(
        "inchikey", "assay_id", "mol_file", "activity", "dataset"
    )

    return data, compound_ids, assay_ids
