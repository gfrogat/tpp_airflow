from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


def standardize_pubchem(spark: SparkSession, compounds_path: Path, assays_path: Path):
    # PubChem - load compounds
    compounds = spark.read.parquet(compounds_path.as_posix())

    # PubChem - load assays
    assays = spark.read.parquet(assays_path.as_posix())

    # Compound IDs
    compound_ids = (
        compounds.select("inchikey", "mol_id")
        .sort(F.asc("mol_id"))
        .withColumn("dataset", F.lit("PubChem"))
    )

    # Assay IDs
    assay_ids = (
        assays.select("aid")
        .distinct()
        .sort(F.asc("aid"))
        .withColumn("assay_id", F.col("aid").cast(T.StringType()))
        .drop("aid")
        .withColumn("dataset", F.lit("PubChem"))
    )

    # PubChem - Merge Compounds and Assays
    data = assays.alias("a").join(
        compounds.alias("c"), F.col("a.cid") == F.col("c.mol_id")
    )

    data = (
        data.withColumn("dataset", F.lit("PubChem"))
        .withColumn("assay_id", F.col("aid").cast(T.StringType()))
        .select("inchikey", "assay_id", "mol_file", "activity", "dataset")
    )

    return data, compound_ids, assay_ids
