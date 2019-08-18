from typing import List

from pyspark.sql import types as T
from rdkit import Chem
from rdkit.Chem import MACCSkeys
import numpy as np


class MACCSFingerprinter(object):
    schema = [T.StructField("maccs_fp", T.ArrayType(T.IntegerType()), True)]

    @staticmethod
    def get_schema() -> List[T.StructField]:
        return MACCSFingerprinter.schema

    @staticmethod
    def calculate(mol: Chem.Mol) -> List[int]:
        result = MACCSkeys.GenMACCSKeys(mol)
        result = np.nonzero(result)[0]
        return result.tolist()
