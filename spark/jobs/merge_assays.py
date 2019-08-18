import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tpp.preprocessing import assay_id_schema
from tpp.preprocessing.chembl import standardize_chembl
from tpp.preprocessing.pubchem import standardize_pubchem

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="PySpark Dataset Merging", description="Merge datasets"
    )
    parser.add_argument(
        "--compounds_chembl",
        required=True,
        type=Path,
        metavar="PATH",
        dest="chembl_compounds_path",
        help="Path to folder with ChEMBL compounds",
    )
    parser.add_argument(
        "--assays_chembl",
        required=True,
        type=Path,
        metavar="PATH",
        dest="chembl_assays_path",
        help="Path to folder with ChEMBL assays",
    )
    parser.add_argument(
        "--compounds_pubchem",
        required=True,
        type=Path,
        metavar="PATH",
        dest="pubchem_compounds_path",
        help="Path to folder with PubChem compounds",
    )
    parser.add_argument(
        "--assays_pubchem",
        required=True,
        type=Path,
        metavar="PATH",
        dest="pubchem_assays_path",
        help="Path to folder with PubChem assays",
    )
    """
    parser.add_argument(
        "--mixed_zinc15",
        required=True,
        type=Path,
        metavar="PATH",
        dest="zinc15_data_path",
        help="Path to folder with ZINC15 data"
    )
    """
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        metavar="PATH",
        dest="output_path",
        help="Path to store merged datasets in `parquet` format",
    )
    parser.add_argument(
        "--merge-all", dest="merge_all", action="store_true", default=False
    )

    args = parser.parse_args()

    try:
        spark = (
            SparkSession.builder.appName("Merge ChEMBL + PubChem")
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        chembl_data, chembl_compound_ids, chembl_assay_ids = standardize_chembl(
            spark, args.chembl_compounds_path, args.chembl_assays_path
        )
        pubchem_data, pubchem_compound_ids, pubchem_assay_ids = standardize_pubchem(
            spark, args.pubchem_compounds_path, args.pubchem_assays_path
        )

        # Export assay_id, global_id mapping
        merged_assay_ids = chembl_assay_ids

        if args.merge_all:
            merged_assays_ids = merged_assay_ids.union(pubchem_assay_ids)

        merged_assay_ids = (
            merged_assay_ids.rdd.zipWithIndex()
            .map(lambda x: (*x[0], x[1]))
            .toDF(assay_id_schema)
        )

        merged_assay_ids.write.parquet(
            (args.output_path / "merged_assay_ids.parquet").as_posix()
        )

        # Export compound_id, inchikey mapping
        merged_compound_ids = chembl_compound_ids
        if args.merge_all:
            merged_compound_ids = merged_compound_ids.union(pubchem_compound_ids)

        merged_compound_ids.write.parquet(
            (args.output_path / "merged_compound_ids.parquet").as_posix()
        )

        # Merge Assays
        merged_data = chembl_data

        if args.merge_all:
            merged_data = merged_data.union(pubchem_data)

        merged_data = (
            merged_data.alias("md")
            .join(
                merged_assay_ids.select("assay_id", "global_id").alias("ma"),
                F.col("md.assay_id") == F.col("ma.assay_id"),
            )
            .select("inchikey", "mol_file", "global_id", "activity", "dataset")
        )

        flattened_data = merged_data.groupby("inchikey").agg(
            F.collect_set("mol_file").alias("mol_file"),
            F.map_from_entries(
                F.sort_array(
                    F.collect_list(F.struct(F.col("global_id"), F.col("activity")))
                ).alias("labels")
            ),
        )

        flattened_data.write.parquet(
            (args.output_path / "merged_data.parquet").as_posix()
        )
    except Exception:
        # handle Exception
        pass
    finally:
        spark.stop()
