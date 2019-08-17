from typing import List

import numpy as np
import pyspark.sql.types as T
from rdkit import Chem


def calculate_rdkit_fp(mol: Chem.Mol) -> List[int]:
    result = Chem.RDKFingerprint(mol, maxPath=6)
    result = np.nonzero(result)[0]
    return result.tolist()


rdkit_fp_schema = [T.StructField("rdkit_fp", T.ArrayType(T.IntegerType()), True)]
