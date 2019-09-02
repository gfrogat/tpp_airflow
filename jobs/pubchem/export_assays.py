import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from tpp.preprocessing.pubchem.assay_parser import PubChemAssayParser
from tpp.utils.argcheck import check_input_path, check_output_path

_pubchem_assay_path = Path("ftp.ncbi.nlm.nih.gov/pubchem/Bioassay/CSV/Data/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="PySpark PubChem Assay Export",
        description="Export PubChem BioAssay data from `csv` to `parquet` format",
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        metavar="PATH",
        dest="pubchem_root_path",
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

    check_input_path(args.pubchem_root_path)
    check_input_path(args.pubchem_root_path / _pubchem_assay_path)
    check_output_path(args.output_path)

    try:
        spark = (
            SparkSession.builder.appName("Process ChEMBL25 Assays")
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        sc = spark.sparkContext

        files = list((args.pubchem_root_path / _pubchem_assay_path).glob("*.zip"))
        files = sc.parallelize(files)

        activities = files.flatMap(PubChemAssayParser.parse_assay).toDF()

        activities.write.parquet(args.output_path.as_posix())
    except Exception as e:
        logging.exception(e)
    finally:
        spark.stop()
