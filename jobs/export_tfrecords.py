import argparse
import json
import logging
from pathlib import Path

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession


def match_datatype(feature):
    if isinstance(feature.dataType, T.MapType):
        return [
            F.map_keys(feature.name).alias(f"{feature.name}_idx"),
            F.map_values(feature.name).alias(f"{feature.name}_val"),
        ]
    elif isinstance(feature.dataType, T.ArrayType):
        return [F.col(feature.name)]
    else:
        raise RuntimeError("Unsupported FeatureType")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Export TFRecords", description=("Export parquet dataset as TFRecords")
    )

    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        metavar="PATH",
        dest="input_path",
        help=f"Path to input `parquet` file",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        metavar="PATH",
        dest="output_dir_path",
        help=f"Path to where `TFRecords` should be stored",
    )
    parser.add_argument(
        "--cluster-mapping",
        required=True,
        type=Path,
        metavar="PATH",
        dest="cluster_mapping_path",
        help=f"Path to cluster mapping in `parquet` format",
    )
    parser.add_argument(
        "--feature", required=True, type=str, action="append", dest="feature_list"
    )

    args = parser.parse_args()

    args.feature_list += ["labels"]

    try:
        spark = (
            SparkSession.builder.appName(parser.prog)
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
        sc = spark.sparkContext

        data = spark.read.parquet(args.input_path.as_posix())

        for feature_name in args.feature_list:
            if feature_name not in data.columns:
                raise ValueError(f"Feature {feature_name} does not exist!")

        feature_query = []

        for feature in data.schema:
            if feature.name in args.feature_list:
                feature_query += match_datatype(feature)

        mapping = spark.read.parquet(args.cluster_mapping_path.as_posix())

        data = data.alias("d").join(
            mapping.alias("m"), F.col("d.inchikey") == F.col("m.inchikey"), how="left"
        )

        data = data.withColumn(
            "cluster", F.when(F.col("cluster_id") == 1.0, 1).otherwise(0)
        )

        data.filter(F.col("cluster_id") == 1).select(F.explode("labels")).groupBy(
            "key"
        ).agg(F.size(F.collect_set("value")).alias("num_activities")).filter(
            F.col("num_activities") == 2
        ).select(
            "key"
        ).sort(
            F.asc("key")
        ).write.parquet(
            (args.output_dir_path / "label_mask_test.parquet").as_posix()
        )

        data.filter(F.col("cluster_id") != 1).select(F.explode("labels")).groupBy(
            "key"
        ).agg(F.size(F.collect_set("value")).alias("num_activities")).filter(
            F.col("num_activities") == 2
        ).select(
            "key"
        ).sort(
            F.asc("key")
        ).write.parquet(
            (args.output_dir_path / "label_mask_train.parquet").as_posix()
        )

        metadata = {}

        for feature in data.schema:
            if feature.name in args.feature_list:
                if isinstance(feature.dataType, T.MapType):
                    metadata[f"{feature.name}_size"] = (
                        data.select(F.explode(feature.name))
                        .select("key")
                        .distinct()
                        .count()
                    )

        records_test = (
            data.filter(F.col("cluster") == 1)
            .drop("cluster_id", "cluster")
            .select(feature_query)
        )
        records_train = (
            data.filter(F.col("cluster") != 1)
            .drop("cluster_id", "cluster")
            .select(feature_query)
        )

        metadata["num_items_test"] = records_test.count()
        metadata["num_items_train"] = records_train.count()

        # Randomize training data and store as TFRecords
        records_test.orderBy(F.rand()).write.format("tfrecords").option(
            "recordType", "Example"
        ).save((args.output_dir_path / "test.tfrecords").as_posix())

        # Randomize testing data and store as TFRecords
        records_train.orderBy(F.rand()).write.format("tfrecords").option(
            "recordType", "Example"
        ).save((args.output_dir_path / "train.tfrecords").as_posix())

        with open(args.output_dir_path / "metadata.json", "w") as f:
            json.dump(metadata, f)

    except Exception as e:
        logging.exception(e)
    finally:
        spark.stop()
