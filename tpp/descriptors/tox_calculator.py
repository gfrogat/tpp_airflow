import pyspark.sql.types as T
from pyspark.sql import Row
from rdkit import Chem

from tpp.descriptors.rdkit import ToxFingerprinter
from tpp.utils import get_socket_logger


class ToxCalculator(object):
    logger = get_socket_logger("ToxCalculator")
    schema = T.StructType(ToxFingerprinter.get_schema())

    @staticmethod
    def calculate_descriptors(molfile):
        mol = Chem.MolFromMolBlock(molfile)

        tox_fp = None

        if mol is not None:
            try:
                tox_fp = ToxFingerprinter.calculate(mol)
            except Exception:
                ToxCalculator.logger.exception("Error computing descriptors")
                tox_fp = None

        row = Row(tox_fp=tox_fp)
        return row
