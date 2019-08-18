from typing import List

from rdkit import Chem
from rdkit.Chem import AllChem
import pyspark.sql.types as T
import numpy as np


class MorganFingerprinter(object):
    schema = [T.StructField("morgan_fp", T.ArrayType(T.IntegerType()), True)]

    @staticmethod
    def get_schema() -> List[T.StructField]:
        return MorganFingerprinter.schema

    @staticmethod
    def calculate(mol: Chem.Mol) -> List[int]:
        result = AllChem.GetMorganFingerprintAsBitVect(mol, 3, nBits=4096)
        result = np.nonzero(result)[0]
        return result.tolist()
