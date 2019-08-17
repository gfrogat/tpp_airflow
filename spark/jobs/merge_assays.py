from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

_data_root = Path("/local00/bioinf/tpp")

merged_assay_ids_schema = T.StructType(
    [
        T.StructField("assay_id", T.StringType(), False),
        T.StructField("dataset", T.StringType(), False),
        T.StructField("global_id", T.IntegerType(), False),
    ]
)


# TODO verify with preprocessed TOX data
def get_zinc15(zinc15_data_path, data_root=_data_root):
    # ZINC15 - load data
    data = spark.read.parquet(
        (data_root / zinc15_data_path).as_posix()
    )

    # Compound IDs
    compound_ids = data \
        .select("inchikey", "mol_id") \
        .dropDuplicates() \
        .sort(F.asc("mol_id"))

    # Assay IDs
    assay_ids = data \
        .select("gene_name") \
        .distinct() \
        .sort(F.asc("gene_name")) \
        .withColumn("assay_id", F.col("gene_name")) \
        .drop("gene_name")

    # Data
    data = data \
        .withColumn("dataset", F.lit("ZINC15")) \
        .withColumn("assay_id", F.col("gene_name")) \
        .select("inchikey", "assay_id", "mol_file", "activity", "dataset")

    return data, compound_ids, assay_ids


def get_pubchem(pubchem_compounds_path, pubchem_assays_path, data_root=_data_root):
    # PubChem - load compounds
    compounds = spark.read.parquet(
        (data_root / pubchem_compounds_path).as_posix()
    )

    # PubChem - load assays
    assays = spark.read.parquet(
        (data_root / pubchem_assays_path).as_posix()
    )

    # Compound IDs
    compound_ids = compounds \
        .select("inchikey", "mol_id") \
        .sort(F.asc("mol_id")) \
        .withColumn("dataset", F.lit("PubChem"))

    # Assay IDs
    assay_ids = assays \
        .select("aid") \
        .distinct() \
        .sort(F.asc("aid")) \
        .withColumn("assay_id", F.col("aid").cast(T.StringType())) \
        .drop("aid") \
        .withColumn("dataset", F.lit("PubChem"))

    # PubChem - Merge Compounds and Assays
    data = assays.alias("a") \
        .join(compounds.alias("c"), F.col("a.cid") == F.col("c.mol_id"))

    data = data \
        .withColumn("dataset", F.lit("PubChem")) \
        .withColumn("assay_id", F.col("aid").cast(T.StringType())) \
        .select("inchikey", "assay_id", "mol_file", "activity", "dataset")

    return data, compound_ids, assay_ids


def get_chembl(chembl_compounds_path, chembl_assays_path, data_root=_data_root):
    # ChEMBL - load compounds
    compounds = spark.read.parquet(
        (data_root / chembl_compounds_path).as_posix()
    )

    # ChEMBL - load assays
    assays = spark.read.parquet(
        (data_root / chembl_assays_path).as_posix()
    )

    # Reformat activity column
    assays = assays \
        .filter(F.col("activity").isin([1, 3])) \
        .withColumn("activity", F.col("activity") - 2)

    # Only use assays with more than 100 measurements
    w = Window.partitionBy("assay_id")
    assays = assays \
        .select("assay_id", "mol_id", "activity") \
        .withColumn("count", F.count("*").over(w)) \
        .filter(F.col("count") > 100) \
        .drop("count") \
        .withColumn("dataset", F.lit("ChEMBL"))

    # Compound IDs
    compound_ids = compounds \
        .select("inchikey", "mol_id") \
        .sort(F.asc("mol_id")) \
        .withColumn("dataset", F.lit("ChEMBL"))

    # Assay IDs
    assay_ids = assays \
        .select("assay_id") \
        .distinct() \
        .sort(F.asc("assay_id")) \
        .withColumn("dataset", F.lit("ChEMBL"))

    # ChEMBL - Merge Compounds and Assays
    data = assays.alias("a") \
        .join(compounds.alias("c"), F.col("a.mol_id") == F.col("c.mol_id"))

    data = data \
        .withColumn("dataset", F.lit("ChEMBL")) \
        .select("inchikey", "assay_id", "mol_file", "activity", "dataset")

    return data, compound_ids, assay_ids


if __name__ == '__main__':
    pubchem_compounds_path = "pubchem_20190717/pubchem_compounds.parquet"
    pubchem_assays_path = "pubchem_20190717/pubchem_processed.parquet"

    chembl_compounds_path = "chembl_25/chembl_25_compounds.parquet"
    chembl_assays_path = "chembl_25/chembl_25_assays_cleaned.parquet"

    zinc15_data_path = "ZINC15/zinc15_data_full.parquet"

    try:
        spark = SparkSession \
            .builder \
            .appName("Merge ChEMBL + PubChem") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        pubchem_data, pubchem_compound_ids, pubchem_assay_ids = get_pubchem(pubchem_compounds_path, pubchem_assays_path)
        chembl_data, chembl_compound_ids, chembl_assay_ids = get_chembl(chembl_compounds_path, chembl_assays_path)

        # Export assay_id, global_id mapping
        merged_assay_ids = pubchem_assay_ids \
            .union(chembl_assay_ids)

        merged_assay_ids = merged_assay_ids \
            .rdd \
            .zipWithIndex() \
            .map(lambda x: (*x[0], x[1])) \
            .toDF(merged_assay_ids_schema)

        merged_assay_ids \
            .write \
            .parquet((_data_root / "merged_assay_ids.parquet").as_posix())

        # Export compound_id, inchikey mapping
        merged_compound_ids = pubchem_compound_ids.union(chembl_compound_ids)
        merged_compound_ids \
            .write \
            .parquet((_data_root / "merged_compound_ids.parquet").as_posix())

        # Merge Assays
        merged_data = pubchem_data.union(chembl_data)
        merged_data = merged_data.alias("md") \
            .join(merged_assay_ids.select("assay_id", "global_id").alias("ma"),
                  F.col("md.assay_id") == F.col("ma.assay_id")) \
            .select("inchikey", "mol_file", "global_id", "activity", "dataset")

        flattened_data = merged_data \
            .groupby("inchikey") \
            .agg(
            F.collect_set("mol_file").alias("mol_file"),
            F.map_from_entries(
                F.sort_array(F.collect_list(
                    F.struct(F.col("global_id"), F.col("activity")))).alias("labels")))

        flattened_data.write.parquet((_data_root / "merged_data.parquet").as_posix())
    except Exception:
    # handle Exception
    finally:
        spark.stop()
