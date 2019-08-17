from typing import List

import numpy as np
import pyspark.sql.types as T
from mordred import Calculator, descriptors
from rdkit import Chem

# `mordred-descriptor` calculator:
# calculates 1613 static descriptors.
calc = Calculator(descriptors, ignore_3D=True, version="1.2.0")

static_features_schema = [
    T.StructField("mordred_features", T.ArrayType(T.IntegerType()), True),
]


def calculate_static_features(mol: Chem.Mol) -> List[float]:
    result = calc(mol)
    result = np.fromiter(result.values(), float)
    return result.tolist()
