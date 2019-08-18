from typing import List

import numpy as np
import pyspark.sql.types as T
from pyspark.ml.linalg import VectorUDT
from mordred import Calculator, descriptors
from rdkit import Chem

# `mordred-descriptor` calculator:
# calculates 1613 static descriptors.
calc = Calculator(descriptors, ignore_3D=True, version="1.2.0")


class StaticFeatures(object):
    schema = [T.StructField("mordred_features", VectorUDT(), True)]

    @staticmethod
    def get_schema() -> List[T.StructField]:
        return StaticFeatures.schema

    @staticmethod
    def calculate(mol: Chem.Mol) -> List[float]:
        result = calc(mol)
        result = np.fromiter(result.values(), float)
        return result.tolist()
