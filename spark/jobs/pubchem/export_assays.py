import csv
import gzip
import zipfile
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql import SparkSession

_data_root = Path("/local00/bioinf/tpp/pubchem_20190717")
_assay_path = _data_root / "ftp.ncbi.nlm.nih.gov/pubchem/Bioassay/CSV/Data/"

activity_outcomes = ["Active", "Inactive"]


def cast_int(string: str):
    try:
        return int(string)
    except (ValueError, TypeError):
        return


def read_csvgz(filepath: Path):
    contents = []

    aid = cast_int(filepath.stem)
    with gzip.open(filepath.as_posix(), mode="rt") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if "RESULT_" not in row["PUBCHEM_RESULT_TAG"]:
                cid, activity_outcome = (
                    cast_int(row["PUBCHEM_CID"]),
                    row["PUBCHEM_ACTIVITY_OUTCOME"],
                )
                if activity_outcome in activity_outcomes:
                    triple = Row(
                        aid=aid,
                        cid=cid,
                        activity_outcome=(3 if activity_outcome == "Active" else 1),
                    )
                    contents.append(triple)

    return contents


def read_zipfile(filepath: Path):
    collection = []

    with zipfile.ZipFile(filepath.as_posix()) as zip:
        zip_info = zip.infolist()
        for zip_content in zip_info:
            with zip.open(zip_content) as csvgz:
                contents = read_csvgz(csvgz)
                if len(contents) > 0:
                    collection += contents

    return collection


if __name__ == '__main__':
    try:
        spark = SparkSession \
            .builder \
            .appName("Process ChEMBL25 Assays") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        sc = spark.sparkContext

        files = list(_assay_path.glob("*.zip"))
        files = sc.parallelize(files)

        activities = files \
            .flatMap(read_zipfile) \
            .toDF()

        activities \
            .write \
            .parquet((_data_root / "pubchem_assays.parquet").as_posix())
    except Exception:
        # handle Exception
    finally:
        spark.stop()
