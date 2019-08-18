import pyspark.sql.types as T
from pyspark.sql import Row
from rdkit import Chem

from tpp.descriptors.mordred import StaticFeatures
from tpp.utils import get_socket_logger


class StaticCalculator(object):
    logger = get_socket_logger("StaticCalculator")
    schema = T.StructType([StaticFeatures.get_schema()])

    @staticmethod
    def get_schema() -> T.StructType:
        return StaticCalculator.schema

    @staticmethod
    def calculate_descriptors(molfile):
        mol = Chem.MolFromMolBlock(molfile)

        static_features = None

        if mol is not None:
            try:
                static_features = StaticFeatures.calculate(mol)
            except Exception:
                StaticCalculator.logger.exception("Error computing descriptors")
                static_features = None

        row = Row(static_features=static_features)
        return row
