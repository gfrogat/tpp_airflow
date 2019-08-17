import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from tpp.preprocessing import Dataset, get_sdf_parser

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog="PySpark SDF Export",
        description="Export SDF to `parquet` format")
    parser.add_argument(
        "--input",
        type=Path,
        metavar="PATH",
        dest="sdf_path",
        help=f"Path to folder with SDF files",
    )
    parser.add_argument(
        "--output",
        type=Path,
        metavar="PATH",
        dest="parquet_path",
        required=True,
        help="Path where output should be written to in `parquet` format",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        type=Dataset,
        dest="dataset",
        choices=list(Dataset)
    )

    args = parser.parse_args()

    sdf_parser = get_sdf_parser(args.dataset)
    schema = sdf_parser.get_schema()

    try:
        spark = SparkSession \
            .builder \
            .appName("Process ChEMBL25 Assays") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        sc = spark.sparkContext

        sdf_files = list(sdf_path)
        sdf_files = sc \
            .parallelize(list(sdf_path)) \
            .repartition(200)

        sdf_parquet = sdf_files \
            .flatMap(sdf_parser.parse_sdf) \
            .toDF(schema=schema)

        sdf_parquet \
            .write \
            .parquet(parquet_path.as_posix())
    except Exception:
    # handle exception
    finally:
        spark.stop()
