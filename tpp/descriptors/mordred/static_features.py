import numpy as np
import pyspark.sql.types as T
from mordred import Calculator, descriptors
from rdkit import Chem

# `mordred-descriptor` calculator:
# calculates 1613 static descriptors.
calc = Calculator(descriptors, ignore_3D=True, version="1.2.0")

static_features_schema = [
    T.StructField("", T.ArrayType(T.IntegerType()), True),
]


def calculate_static_features(mol: Chem.Mol):
    result = calc(mol)
    result = np.array(list(result)).astype(np.float)
    return result.tolist()
