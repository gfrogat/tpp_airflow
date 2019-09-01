from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


# TODO verify with preprocessed ZINC15 data
def get_zinc15(spark: SparkSession, data_path: Path):
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

    return data, compound_ids, assay_ids
