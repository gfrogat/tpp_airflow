import argparse
import logging
from pathlib import Path

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from tpp.descriptors import FeatureType, get_feature_calculator
from tpp.utils.argcheck import check_input_path, check_output_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Descriptor Computatian",
        description="Compute descriptors for flattened data in `parquet` format.",
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

    check_input_path(args.input_path)
    check_output_path(args.output_path)

    calculator = get_feature_calculator(args.feature_type)
    schema = calculator.get_schema()

    calculate_features = F.udf(calculator.calculate_descriptors, schema)

    try:
        spark = (
            SparkSession.builder.appName(parser.prog)
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

    except Exception as e:
        logging.exception(e)
    finally:
        spark.stop()
