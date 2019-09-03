import pyspark.sql.types as T
from pyspark.sql import Row
from rdkit import Chem

from tpp.descriptors.rdkit import MorganFingerprinter
from tpp.utils import get_socket_logger


class MorganCalculator(object):
    logger = get_socket_logger("MorganCalculator")
    schema = T.StructType(MorganFingerprinter.get_schema())

    @staticmethod
    def get_schema() -> T.StructType:
        return MorganCalculator.schema

    @staticmethod
    def calculate_descriptors(molfile):
        mol = Chem.MolFromMolBlock(molfile)

        morgan_fp = None

        if mol is not None:
            try:
                morgan_fp = MorganFingerprinter.calculate(mol)
            except Exception:
                MorganCalculator.logger.exception("Error computing descriptors")
                morgan_fp = None

        row = Row(morgan_fp=morgan_fp)
        return row
