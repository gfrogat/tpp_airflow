import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from tpp.preprocessing import Dataset, get_sdf_parser
from tpp.utils.argcheck import check_input_path, check_output_path

_pubchem_compound_path = Path("ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="SDF Export", description="Export SDF into `parquet` format"
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        metavar="PATH",
        dest="input_path",
        help=f"Path to folder with SDF files",
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
        "--dataset", required=True, type=Dataset, dest="dataset", choices=list(Dataset)
    )
    parser.add_argument(
        "--num-partitions", type=int, dest="num_partitions", default=200
    )

    args = parser.parse_args()

    check_input_path(args.input_path)
    check_output_path(args.output_path)

    sdf_parser = get_sdf_parser(args.dataset)
    schema = sdf_parser.get_schema()

    input_path = args.input_path
    glob_pattern = "*.sdf"

    if args.dataset == Dataset.PUBCHEM:
        input_path = args.input_path / _pubchem_compound_path
        glob_pattern = "*.sdf.gz"

    try:
        spark = (
            SparkSession.builder.appName("Process ChEMBL25 Assays")
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        sc = spark.sparkContext

        sdf_files = list(input_path.glob(glob_pattern))
        sdf_files = sc.parallelize(sdf_files).repartition(args.num_partitions)

        sdf_parquet = sdf_files.flatMap(sdf_parser.parse_sdf).toDF(schema=schema)

        sdf_parquet.write.parquet(args.output_path.as_posix())
    except Exception as e:
        logging.exception(e)
    finally:
        spark.stop()
