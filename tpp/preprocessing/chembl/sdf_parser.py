from pathlib import Path
from typing import List

from pyspark.sql import Row
from pyspark.sql import types as T
from rdkit import Chem

from tpp.utils import get_socket_logger

from .. import parser


class ChEMBLSDFParser(parser.SDFParser):
    schema = T.StructType(
        [
            T.StructField("mol_id", T.StringType(), False),
            T.StructField("inchikey", T.StringType(), False),
            T.StructField("mol_file", T.StringType(), False),
        ]
    )
    logger = get_socket_logger("ChEMBLSDFParser")

    @staticmethod
    def get_schema() -> T.StructType:
        return ChEMBLSDFParser.schema

    @staticmethod
    def parse_sdf(sdf_path: Path) -> List[Row]:
        res = []

        suppl = Chem.SDMolSupplier(sdf_path.as_posix())

        for mol in suppl:
            if mol is not None:
                try:
                    chembl_id = mol.GetProp("chembl_id")
                    inchikey = Chem.MolToInchiKey(mol)
                    mol_block = Chem.MolToMolBlock(mol)
                    row = Row(mol_id=chembl_id, inchikey=inchikey, mol_file=mol_block)
                    res.append(row)
                except Exception:
                    if "chembl_id" in locals():
                        ChEMBLSDFParser.logger.exception(f"Error parsing {chembl_id}")
                    else:
                        ChEMBLSDFParser.logger.exception("Error parsing UNKNOWN")

        return res
