import gzip
from pathlib import Path
from typing import List

from pyspark.sql import Row
from pyspark.sql import types as T
from rdkit import Chem

from tpp.utils import get_socket_logger

from .. import parser


class PubChemSDFParser(parser.SDFParser):
    schema = T.StructType(
        [
            T.StructField("mol_id", T.StringType(), False),
            T.StructField("inchikey", T.StringType(), False),
            T.StructField("mol_file", T.StringType(), False),
        ]
    )
    logger = get_socket_logger("PubChemSDFParser")

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
                        row = Row(mol_id=cid, inchikey=inchikey, mol_file=mol_block)
                        res.append(row)
                    except Exception:
                        if "cid" in locals():
                            PubChemSDFParser.logger.exception(f"Error parsing {cid}")
                        else:
                            PubChemSDFParser.logger.exception("Error parsing UNKNOWN")

        return res
