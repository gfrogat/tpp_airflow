from typing import List

import numpy as np
import pyspark.sql.types as T
from rdkit import Chem


class RDKitFingerprinter(object):
    schema = [T.StructField("rdkit_fp", T.ArrayType(T.IntegerType()), True)]

    @staticmethod
    def get_schema() -> List[T.StructField]:
        return RDKitFingerprinter.schema

    @staticmethod
    def calculate(mol: Chem.Mol) -> List[int]:
        result = Chem.RDKFingerprint(mol, maxPath=6)
        result = np.nonzero(result)[0]
        return result.tolist()
