from enum import Enum

from .parser import SDFParser
from .chembl.parser import ChEMBLSDFParser
from .pubchem.parser import PubChemSDFParser
from .zinc15.parser import ZINC15SDFParser


class Dataset(Enum):
    CHEMBL = "ChEMBL"
    PUBCHEM = "PubChem"
    ZINC15 = "ZINC15"

    def __str__(self) -> str:
        return self.value


def get_sdf_parser(dataset: Dataset):
    if dataset == Dataset.CHEMBL:
        return ChEMBLSDFParser
    elif dataset == Dataset.PUBCHEM:
        return PubChemSDFParser
    elif dataset == Dataset.ZINC15:
        return ZINC15SDFParser
