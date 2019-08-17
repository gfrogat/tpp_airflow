import csv
import gzip
import zipfile
from pathlib import Path
from typing import List

from pyspark.sql import Row
from pyspark.sql import types as T
from rdkit import Chem

from .. import parser


class PubChemSDFParser(parser.SDFParser):
    schema = T.StructType(
        [
            T.StructField("mol_id", T.StringType(), False),
            T.StructField("inchikey", T.StringType(), False),
            T.StructField("mol_file", T.StringType(), False),
        ]
    )

    @staticmethod
    def get_schema() -> T.StructType:
        return PubChemSDFParser.schema

    @staticmethod
    def parse_sdf(sdf_path: Path) -> List[Row]:
        res = []

        with gzip.open(sdf_path.as_posix(), "rb") as f:
            suppl = Chem.ForwardSDMolSupplier(f)
            for mol in suppl:
                if mol is not None:
                    try:
                        cid = mol.GetProp("PUBCHEM_COMPOUND_CID")
                        inchikey = Chem.MolToInchiKey(mol)
                        mol_block = Chem.MolToMolBlock(mol)
                        row = Row(
                            mol_id=cid,
                            inchikey=inchikey,
                            mol_file=mol_block)
                        res.append(row)
                    except Exception:
                        pass

        return res


class PubChemAssayParser(parser.AssayParser):
    activity_outcomes = ["Active", "Inactive"]

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
                            row = Row(
                                aid=aid,
                                cid=cid,
                                activity=activity)
                            res.append(row)
                    except (ValueError, TypeError):
                        pass

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
