import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from tpp.utils.argcheck import check_input_path, check_output_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="PySpark PubChem Assay Processing",
        description="Process PubChem BioAssay data in `parquet` format",
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        metavar="PATH",
        dest="input_path",
        help=f"Path to PubChem folder (FTP schema)",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        metavar="PATH",
        dest="output_path",
        help="Path where output should be written to in `parquet` format",
    )

    args = parser.parse_args()

    check_input_path(args.input_path)
    check_output_path(args.output_path)

    try:
        spark = (
            SparkSession.builder.appName("Process ZINC15 Assays")
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        df = spark.read.parquet(args.input_path.as_posix())
        df = df.dropna()

        # Remove non-unique inchikey per mol_id collisions
        w = Window.partitionBy("mol_id")
        df = df.withColumn(
            "num_inchikey", F.size(F.collect_set("inchikey").over(w))
        ).filter(F.col("num_inchikey") == 1)

        triples = df.groupby(["mol_id", "gene_name"]).agg(
            F.mean("affinity").alias("avg_affinity"),
            F.collect_set("mol_file").alias("mol_file"),
            F.collect_set("inchikey").alias("inchikey"),
        )

        triples = triples.withColumn(
            "activity", F.when(F.col("avg_affinity") > 8, 1).otherwise(-1)
        )

        w = Window.partitionBy("gene_name")
        triples = (
            triples.withColumn(
                "actives", F.count(F.when(F.col("activity") == 1, 0)).over(w)
            )
            .withColumn(
                "inactives", F.count(F.when(F.col("activity") == -1, 0)).over(w)
            )
            .filter(
                (F.col("actives") > 10)
                & (F.col("inactives") > 10)
                & (F.col("actives") + F.col("inactives") > 25)
            )
            .select("mol_id", "inchikey", "mol_file", "gene_name", "activity")
        )

        triples.write.parquet(args.output_path.as_posix())
    except Exception as e:
        logging.exception(e)
    finally:
        spark.stop()
