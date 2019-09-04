import argparse
import logging
from pathlib import Path

import pyspark.sql.functions as F

from pyspark.sql import SparkSession

from tpp.preprocessing.chembl.standardize_chembl import standardize_chembl
from tpp.preprocessing.pubchem.standardize_pubchem import standardize_pubchem
from tpp.preprocessing.schemas import (
    assay_id_schema,
    compound_id_schema,
    merged_data_schema,
)
from tpp.preprocessing.zinc15.standardize_zinc15 import standardize_zinc15
from tpp.utils.argcheck import (
    check_arguments_chembl,
    check_arguments_pubchem,
    check_arguments_zinc15,
    check_path,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Dataset Merging",
        description=(
            "Merge multiple processed datasets and output flattened dataset "
            "for feature computation in `parquet` format."
        ),
    )

    parser.add_argument("--merge-chembl", action="store_true", default=False)
    parser.add_argument(
        "--chembl-compounds",
        type=Path,
        metavar="PATH",
        dest="chembl_compounds_path",
        help="Path to folder with ChEMBL compounds",
    )
    parser.add_argument(
        "--chembl-assays",
        type=Path,
        metavar="PATH",
        dest="chembl_assays_path",
        help="Path to folder with ChEMBL compounds",
    )

    parser.add_argument("--merge-pubchem", action="store_true", default=False)
    parser.add_argument(
        "--pubchem-compounds",
        type=Path,
        metavar="PATH",
        dest="pubchem_compounds_path",
        help="Path to folder with PubChem compounds",
    )
    parser.add_argument(
        "--pubchem-assays",
        type=Path,
        metavar="PATH",
        dest="pubchem_assays_path",
        help="Path to folder with PubChem assays",
    )

    parser.add_argument("--merge-zinc15", action="store_true", default=False)
    parser.add_argument(
        "--zinc15-data",
        type=Path,
        metavar="PATH",
        dest="zinc15_data_path",
        help="Path to folder with ZINC15 (combined) compounds/assays data",
    )

    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        metavar="PATH",
        dest="output_dir_path",
        help="Path to store merged datasets in `parquet` format",
    )

    args = parser.parse_args()

    if not (args.merge_chembl or args.merge_pubchem or args.merge_zinc15):
        parser.error(
            (
                "Specify at least one dataset `--merge-{chembl, pubchem, zinc15}`"
                " that should be merged!"
            )
        )

    check_arguments_chembl(args)
    check_arguments_pubchem(args)
    check_arguments_zinc15(args)

    check_path(args.output_dir_path)

    try:
        spark = (
            SparkSession.builder.appName(parser.prog)
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
        sc = spark.sparkContext

        # Export assay_id, global_id mapping
        merged_assay_ids = sc.emptyRDD().toDF(schema=assay_id_schema)
        # Drop global_id because we will add it later after
        merged_assay_ids = merged_assay_ids.drop("global_id")

        # Export compound_id, inchikey mapping
        merged_compound_ids = sc.emptyRDD().toDF(schema=compound_id_schema)

        # Merge Assays
        merged_data = sc.emptyRDD().toDF(schema=merged_data_schema)

        if args.merge_chembl:
            chembl_data, chembl_compound_ids, chembl_assay_ids = standardize_chembl(
                spark, args.chembl_compounds_path, args.chembl_assays_path
            )
            merged_data = merged_data.union(chembl_data)
            merged_compound_ids = merged_compound_ids.union(chembl_compound_ids)
            merged_assay_ids = merged_assay_ids.union(chembl_assay_ids)

        if args.merge_pubchem:
            pubchem_data, pubchem_compound_ids, pubchem_assay_ids = standardize_pubchem(
                spark, args.pubchem_compounds_path, args.pubchem_assays_path
            )
            merged_data = merged_data.union(pubchem_data)
            merged_compound_ids = merged_compound_ids.union(pubchem_compound_ids)
            merged_assay_ids = merged_assay_ids.union(pubchem_assay_ids)

        if args.merge_zinc15:
            zinc15_data, zinc15_compound_ids, zinc15_assay_ids = standardize_zinc15(
                spark, args.zinc15_data_path
            )

            merged_data = merged_data.union(zinc15_data)
            merged_compound_ids = merged_compound_ids.union(zinc15_compound_ids)
            merged_assay_ids = merged_assay_ids.union(zinc15_assay_ids)

        # Write Compound IDs to parquet
        merged_compound_ids.write.parquet(
            (args.output_dir_path / "merged_compound_ids.parquet").as_posix()
        )

        # Numerate Assay_ids and write to parquet
        merged_assay_ids = (
            merged_assay_ids.rdd.zipWithIndex()
            .map(lambda x: (*x[0], x[1]))
            .toDF(assay_id_schema)
        )

        merged_assay_ids.write.parquet(
            (args.output_dir_path / "merged_assay_ids.parquet").as_posix()
        )

        # Merge and flatten assays / compounds pairings
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
                )
            ).alias("labels"),
        )

        flattened_data.write.parquet(
            (args.output_dir_path / "merged_data.parquet").as_posix()
        )
    except Exception as e:
        logging.exception(e)
    finally:
        spark.stop()
