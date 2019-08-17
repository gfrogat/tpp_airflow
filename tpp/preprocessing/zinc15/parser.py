from pathlib import Path
from typing import List

from pyspark.sql import Row
from pyspark.sql import types as T
from rdkit import Chem

from .. import parser


class ZINC15SDFParser(parser.SDFParser):
    schema = T.StructType(
        [
            T.StructField("mol_id", T.StringType(), False),
            T.StructField("inchikey", T.StringType(), False),
            T.StructField("mol_file", T.StringType(), False),
            T.StructField("gene_name", T.StringType(), False),
            T.StructField("affinity", T.DoubleType(), False),
        ]
    )

    @staticmethod
    def get_schema() -> T.StructType:
        return ZINC15SDFParser.schema

    @staticmethod
    def parse_sdf(sdf_path: Path) -> List[Row]:
        res = []

        suppl = Chem.SDMolSupplier(sdf_path.as_posix())

        for mol in suppl:
            if mol is not None:
                try:
                    zinc_id = mol.GetProp("zinc_id")
                    inchikey = Chem.MolToInchiKey(mol)
                    mol_block = Chem.MolToMolBlock(mol)
                    gene_name = mol.GetProp("gene_name")
                    affinity = float(mol.GetProp("affinity"))
                    row = Row(
                        mol_id=zinc_id,
                        inchikey=inchikey,
                        mol_file=mol_block,
                        gene_name=gene_name,
                        affinity=affinity)
                    res.append(row)
                except Exception:
                    pass

        return res
