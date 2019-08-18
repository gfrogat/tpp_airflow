import csv
import gzip
import zipfile
from pathlib import Path
from typing import List

from pyspark.sql import Row

from tpp.utils import get_socket_logger

from .. import parser


class PubChemAssayParser(parser.AssayParser):
    activity_outcomes = ["Active", "Inactive"]
    logger = get_socket_logger("PubChemAssayParser")

    @staticmethod
    def _cast_int(string: str) -> int:
        return int(string)

    @staticmethod
    def _read_csvgz(filepath: Path) -> List[Row]:
        res = []

        aid = PubChemAssayParser._cast_int(filepath.stem)
        with gzip.open(filepath, mode="rt") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if "RESULT_" not in row["PUBCHEM_RESULT_TAG"]:
                    try:
                        cid = PubChemAssayParser._cast_int(row["PUBCHEM_CID"])
                        activity_outcome = row["PUBCHEM_ACTIVITY_OUTCOME"]
                        if activity_outcome in PubChemAssayParser.activity_outcomes:
                            activity = 3 if activity_outcome == "Active" else 1
                            row = Row(aid=aid, cid=cid, activity=activity)
                            res.append(row)
                    except Exception:
                        if "cid" in locals():
                            PubChemAssayParser.logger.exception(f"Error parsing {cid}")
                        else:
                            PubChemAssayParser.logger.exception("Error parsing UNKNOWN")

        return res

    @staticmethod
    def _read_zipfile(filepath: Path) -> List[Row]:
        collection = []

        with zipfile.ZipFile(filepath) as zip_file:
            zip_info = zip_file.infolist()
            for zip_content in zip_info:
                with zip_file.open(zip_content) as csvgz:
                    contents = PubChemAssayParser._read_csvgz(csvgz)
                    if len(contents) > 0:
                        collection += contents

        return collection

    @staticmethod
    def parse_assay(filepath: Path):
        return PubChemAssayParser._read_zipfile(filepath)
