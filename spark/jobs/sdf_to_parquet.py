import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from tpp.preprocessing import Dataset, get_sdf_parser

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="PySpark SDF Export", description="Export SDF to `parquet` format"
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        metavar="PATH",
        dest="sdf_path",
        help=f"Path to folder with SDF files",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        metavar="PATH",
        dest="parquet_path",
        help="Path where output should be written to in `parquet` format",
    )
    parser.add_argument(
        "--dataset", required=True, type=Dataset, dest="dataset", choices=list(Dataset)
    )
    parser.add_argument(
        "--num-partitions", type=int, dest="num_partitions", default=200
    )

    args = parser.parse_args()

    if not args.sdf_path.exists():
        raise FileNotFoundError(f"Path {args.sdf_path} does not exist")

    if not args.parquet_path.parent.exists():
        raise FileNotFoundError(
            f"Parent folder {args.parquet_path.parent} does not exist!"
        )

    if args.parquet_path.exists():
        raise FileExistsError(f"{args.parquet_path} already exists!")

    sdf_parser = get_sdf_parser(args.dataset)
    schema = sdf_parser.get_schema()

    try:
        spark = (
            SparkSession.builder.appName("Process ChEMBL25 Assays")
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        sc = spark.sparkContext

        sdf_files = list(args.sdf_path.glob("*.sdf"))
        sdf_files = sc.parallelize(sdf_files).repartition(args.num_partitions)

        sdf_parquet = sdf_files.flatMap(sdf_parser.parse_sdf).toDF(schema=schema)

        sdf_parquet.write.parquet(args.parquet_path.as_posix())
    except Exception:
        # handle exception
        pass
    finally:
        spark.stop()
