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

        static_features = None

        if mol is not None:
            try:
                static_features = MorganFingerprinter.calculate(mol)
            except Exception:
                MorganCalculator.logger.exception("Error computing descriptors")
                static_features = None

        row = Row(static_features=static_features)
        return row
