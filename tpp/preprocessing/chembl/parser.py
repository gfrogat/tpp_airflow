from pathlib import Path
from typing import List

from pyspark.sql import types as T
from pyspark.sql import Row
from rdkit import Chem

from .. import parser


class ChEMBLSDFParser(parser.SDFParser):
    schema = T.StructType(
        [
            T.StructField("mol_id", T.StringType(), False),
            T.StructField("inchikey", T.StringType(), False),
            T.StructField("mol_file", T.StringType(), False),
        ]
    )

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
                    row = Row(
                        mol_id=chembl_id,
                        inchikey=inchikey,
                        mol_file=mol_block)
                    res.append(row)
                except Exception:
                    pass

        return res
