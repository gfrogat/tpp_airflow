import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

from tpp.descriptors.cleaner import clean_frequency_feature
from tpp.utils.argcheck import check_input_path, check_output_path, check_path


def clean_feature(df: DataFrame, feature_name: str, output_dir_path: Path):
    clean_features, feature_ids = clean_frequency_feature(df, feature_name)

    feature_ids.write.parquet(
        (output_dir_path / "{}_ids.parquet".format(feature_name)).as_posix()
    )

    df = (
        df.alias("df")
        .join(
            clean_features.alias("clean"),
            F.col("df.inchikey") == F.col("clean.inchikey"),
        )
        .select("df.*", "clean.{}_clean".format(feature_name))
        .drop(feature_name)
    )

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Frequency Fingerprint Cleaning",
        description=(
            "Clean Frequency Fingerprint features. Replace feature smart "
            "strings with numeric id and remove infrequent features."
        ),
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
        "--output-dir",
        required=True,
        type=Path,
        metavar="PATH",
        dest="output_dir_path",
        help=(
            "Path where output and computed features should be written to "
            "in `parquet` format"
        ),
    )
    parser.add_argument(
        "--temp-files",
        required=True,
        type=Path,
        metavar="PATH",
        dest="temp_files_path",
        default="/local00/bioinf/spark/tmp",
        help=("Path where temporary files are stored " "in `parquet` format"),
    )
    parser.add_argument(
        "--feature", required=True, type=str, action="append", dest="feature_list"
    )
    args = parser.parse_args()

    check_input_path(args.input_path)
    check_output_path(args.output_dir_path)
    check_path(args.temp_files_path)

    try:
        spark = (
            SparkSession.builder.appName(parser.prog)
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.executor.memory", "5g")
            .getOrCreate()
        )

        data_mixed = spark.read.parquet(args.input_path.as_posix())

        for feature_name in args.feature_list:
            if feature_name not in data_mixed.columns:
                raise ValueError(f"Feature {feature_name} does not exist!")

        for idx, feature_name in enumerate(args.feature_list):
            data_mixed = clean_feature(data_mixed, feature_name, args.output_dir_path)
            data_mixed.write.parquet(
                (
                    args.temp_files_path / "clean_features_chkpt{}.parquet".format(idx)
                ).as_posix()
            )
            data_mixed = spark.read.parquet(
                (
                    args.temp_files_path / "clean_features_chkpt{}.parquet".format(idx)
                ).as_posix()
            )

        data_mixed.write.parquet(
            (args.output_dir_path / "data_clean.parquet").as_posix()
        )
    except Exception as e:
        logging.exception(e)
    finally:
        spark.stop()
