import os
import gzip
import zipfile
import csv
import glob
from pyspark.sql import Row

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Export PubChem Compounds")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

sc = spark.sparkContext


def cast_int(string):
    try:
        return int(string)
    except (ValueError, TypeError):
        return


def read_csvgz(file):
    contents = []

    filename = os.path.basename(file.name).split(".")[0]
    aid = cast_int(filename)
    with gzip.open(file, mode="rt") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if "RESULT_" not in row["PUBCHEM_RESULT_TAG"]:
                cid, activity_outcome = (
                    cast_int(row["PUBCHEM_CID"]),
                    row["PUBCHEM_ACTIVITY_OUTCOME"],
                )
                if activity_outcome in ["Active", "Inactive"]:
                    triple = Row(
                        aid=aid,
                        cid=cid,
                        activity_outcome=(3 if activity_outcome == "Active" else 1),
                    )
                    contents.append(triple)

    return contents


def read_zipfile(direntry):
    collection = []

    with zipfile.ZipFile(direntry) as zipdir:
        zipdir_contents = zipdir.infolist()
        for zipdir_content in zipdir_contents:
            with zipdir.open(zipdir_content) as content:
                contents = read_csvgz(content)
                if len(contents) > 0:
                    collection += contents

    return collection


filepath = "/local00/bioinf/tpp/ftp.ncbi.nlm.nih.gov/pubchem/Bioassay/CSV/Data/"
files = glob.glob(filepath + "/*.zip")

files = sc.parallelize(files)

result = files.flatMap(read_zipfile)
result = result.toDF()

result.write.parquet("/local00/bioinf/tpp/pubchem_20190717/pubchem.parquet")
