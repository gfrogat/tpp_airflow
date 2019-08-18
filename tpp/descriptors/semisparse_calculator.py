import pyspark.sql.types as T
from pyspark.sql import Row
from rdkit import Chem

from tpp.descriptors.rdkit import MACCSFingerprinter, RDKitFingerprinter
from tpp.utils import get_socket_logger


class SemiSparsCalculator(object):
    logger = get_socket_logger("SemiSparseCalculator")
    schema = T.StructType(
        MACCSFingerprinter.get_schema() + RDKitFingerprinter.get_schema()
    )

    @staticmethod
    def calculate_descriptors(molfile):
        mol = Chem.MolFromMolBlock(molfile)

        rdkit_fp = None
        maccs_fp = None

        if mol is not None:
            try:
                rdkit_fp = RDKitFingerprinter.calculate(mol)
                maccs_fp = MACCSFingerprinter.calculate(mol)
            except Exception:
                SemiSparsCalculator.logger.exception("Error computing descriptors")
                rdkit_fp = None
                maccs_fp = None

        row = Row(rdkit_fp=rdkit_fp, maccs_fp=maccs_fp)
        return row
