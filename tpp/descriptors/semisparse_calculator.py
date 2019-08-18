import pyspark.sql.types as T
from pyspark.sql import Row
from rdkit import Chem

from tpp.descriptors.rdkit import (
    calculate_maccs_fp,
    calculate_rdkit_fp,
    maccs_fp_schema,
    rdkit_fp_schema,
)
from tpp.utils import get_socket_logger


class SemiSparsCalculator(object):
    logger = get_socket_logger("SemiSparseCalculator")
    schema = T.StructType(rdkit_fp_schema + maccs_fp_schema)

    @staticmethod
    def calculate_descriptors(molfile):
        mol = Chem.MolFromMolBlock(molfile)

        rdkit_fp = None
        maccs_fp = None

        if mol is not None:
            try:
                rdkit_fp = calculate_rdkit_fp(mol)
                maccs_fp = calculate_maccs_fp(mol)
            except Exception:
                SemiSparsCalculator.logger.exception("Error computing descriptors")
                rdkit_fp = None
                maccs_fp = None

        row = Row(rdkit_fp=rdkit_fp, maccs_fp=maccs_fp)
        return row
