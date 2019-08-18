from typing import List

from rdkit import Chem
import pyspark.sql.types as T

from .tox_smarts import tox_smarts


def setup_smarts() -> List[Chem.Mol]:
    smarts = [Chem.MolFromSmarts(tm) for tm in tox_smarts]
    smarts = [tm for tm in smarts if tm is not None]
    return smarts


class ToxFingerprinter(object):
    schema = [T.StructField("tox_fp", T.ArrayType(T.IntegerType()), True)]
    smarts = setup_smarts()

    @staticmethod
    def get_schema():
        return ToxFingerprinter.schema

    @staticmethod
    def calculate(mol: Chem.Mol) -> List[int]:
        result = []

        for i, tm in enumerate(ToxFingerprinter.smarts):
            present = mol.HasSubstructMatch(tm)
            if present:
                result.append(i)

        return result
