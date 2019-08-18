import pyspark.sql.types as T
from pyspark.sql import Row
from rdkit import Chem

from tpp.descriptors.rdkit.tox import calculate_tox_fp, tox_fp_schema
from tpp.utils import get_socket_logger


class ToxCalculator(object):
    logger = get_socket_logger("ToxCalculator")
    schema = T.StructType(tox_fp_schema)

    @staticmethod
    def calculate_descriptors(molfile):
        mol = Chem.MolFromMolBlock(molfile)

        tox_fp = None

        if mol is not None:
            try:
                tox_fp = calculate_tox_fp(mol)
            except Exception:
                ToxCalculator.logger.exception("Error computing descriptors")
                tox_fp = None

        row = Row(tox_fp=tox_fp)
        return row
