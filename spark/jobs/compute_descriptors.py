import argparse
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from tpp.descriptors import FeatureType, get_feature_calculator

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="PySpark Feature Computation",
        description="Compute descriptors for `parquet` data.",
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        metavar="PATH",
        dest="input_path",
        help=f"Path to folder with input `parquet` file",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        metavar="PATH",
        dest="output_path",
        help=(
            "Path where output and computed features should be written to "
            "in `parquet` format"
        ),
    )
    parser.add_argument(
        "--feature-type",
        required=True,
        type=FeatureType,
        dest="feature_type",
        choices=list(FeatureType),
    )
    parser.add_argument(
        "--num-partitions", type=int, dest="num_partitions", default=200
    )

    args = parser.parse_args()

    if not args.input_path.exists():
        raise FileNotFoundError(f"Path {args.input_path} does not exist")

    if not args.output_path.parent.exists():
        raise FileNotFoundError(
            f"Parent folder {args.output_path.parent} does not exist!"
        )

    if args.output_path.exists():
        raise FileExistsError(f"{args.output_path} already exists!")

    calculator = get_feature_calculator(args.feature_type)
    schema = calculator.get_schema()

    calculate_features = F.udf(calculator.calculate_descriptors, schema)

    try:
        spark = (
            SparkSession.builder.appName("Compute Mordred Fingerprints")
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        df = spark.read.parquet(args.input_path.as_posix()).repartition(
            args.num_partitions
        )

        column_names = df.schema.names
        descriptor_names = ["descriptors.{}".format(name) for name in schema.names]
        query_names = column_names + descriptor_names

        df = df.withColumn(
            "descriptors", calculate_features(F.col("mol_file").getItem(0))
        )
        df = df.select(*query_names)

        df.write.parquet(args.output_path.as_posix())

    except Exception:
        pass
    finally:
        spark.stop()
