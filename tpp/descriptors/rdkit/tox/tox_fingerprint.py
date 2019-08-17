from rdkit import Chem
from .tox_smarts import tox_smarts

smarts = [Chem.MolFromSmarts(tm) for tm in tox_smarts]
smarts = [tm for tm in smarts if tm is not None]


def calculate_tox_fp(mol: Chem.Mol):
    result = []

    for i, tm in enumerate(smarts):
        present = mol.HasSubstructMatch(tm)
        if present:
            result.append(i)

    return result


schema_tox_fp = [
    T.StructField("tox_fp", T.ArrayType(T.IntegerType()), True),
]
