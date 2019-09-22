import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T

from tpp.utils.argcheck import check_input_path, check_output_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="PubChem Assay Processing",
        description="Process PubChem BioAssay data in `parquet` format",
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        metavar="PATH",
        dest="input_path",
        help=f"Path to folder with PubChem assays in `parquet' format",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        metavar="PATH",
        dest="output_path",
        help="Path where output should be written to in `parquet` format",
    )
    parser.add_argument(
        "--num-partitions", type=int, dest="num_partitions", default=200
    )

    args = parser.parse_args()

    check_input_path(args.input_path)
    check_output_path(args.output_path)

    try:
        spark = (
            SparkSession.builder.appName(parser.prog)
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        assays = spark.read.parquet(args.input_path.as_posix())
        assays = assays.repartition(args.num_partitions)
        assays = assays.dropna()

        # remove assays with few measurements
        w = Window.partitionBy("aid")

        # Filter out assays that don't appear at least 25 times
        assays = (
            assays.select("aid", "cid", "activity")
            .withColumn("count", F.count("*").over(w))
            .filter(F.col("count") > 25)
            .drop("count")
        )

        assays = assays.withColumn(
            "affinity", F.when(F.col("activity") == 3, 1.0).otherwise(0.0)
        ).drop("activity")

        assays = (
            assays.groupby(["aid", "cid"])
            .agg(F.mean(F.col("affinity")).alias("avg_affinity"))
            .filter(F.col("avg_affinity") != 0.5)
        )

        assays = assays.withColumn(
            "activity",
            (2 * (F.round(F.col("avg_affinity")) - 0.5)).cast(T.IntegerType()),
        ).drop("avg_affinity")

        processed_assays = (
            assays.withColumn(
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
            .select("cid", "aid", "activity")
        )

        processed_assays.write.parquet(args.output_path.as_posix())
    except Exception as e:
        logging.exception(e)
        raise SystemExit(
            "Spark Job encountered a problem. Check the logs for more information"
        )
    finally:
        spark.stop()
