from rdkit import Chem
import pyspark.sql.types as T
import numpy as np


def calculate_rdkit_fp(mol: Chem.Mol):
    result = Chem.RDKFingerprint(mol, maxPath=6)
    result = np.nonzero(result)[0]
    return result.tolist()


rdkit_fp_schema = [
    T.StructField("rdkit_fp", T.ArrayType(T.IntegerType()), True)
]
