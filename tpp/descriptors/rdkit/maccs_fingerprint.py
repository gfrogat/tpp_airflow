from typing import List

from pyspark.sql import types as T
from rdkit import Chem
from rdkit.Chem import MACCSkeys
import numpy as np


def calculate_maccs_fp(mol: Chem.Mol) -> List[int]:
    result = MACCSkeys.GenMACCSKeys(mol)
    result = np.nonzero(result)[0]
    return result.tolist()


maccs_fp_schema = [
    T.StructField("maccs_fp", T.ArrayType(T.IntegerType()), True),
]
