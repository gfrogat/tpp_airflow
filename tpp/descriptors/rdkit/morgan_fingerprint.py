from rdkit import Chem
from rdkit.Chem import AllChem
import pyspark.sql.types as T


def calculate_morgan_fp(mol: Chem.Mol):
    result = AllChem.GetMorganFingerprintAsBitVect(mol, 3, nBits=4096)
    result = np.nonzero(result)[0]
    return result.tolist()


morgan_fp_schema = [
    T.StructField("morgan_fp", T.ArrayType(T.IntegerType()), True),
]
