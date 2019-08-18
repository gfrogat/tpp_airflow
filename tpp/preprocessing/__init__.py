from enum import Enum


class Dataset(Enum):
    CHEMBL = "ChEMBL"
    PUBCHEM = "PubChem"
    ZINC15 = "ZINC15"

    def __str__(self) -> str:
        return self.value


def get_sdf_parser(dataset: Dataset):
    if dataset == Dataset.CHEMBL:
        from .chembl.sdf_parser import ChEMBLSDFParser

        return ChEMBLSDFParser
    elif dataset == Dataset.PUBCHEM:
        from .pubchem.sdf_parser import PubChemSDFParser

        return PubChemSDFParser
    elif dataset == Dataset.ZINC15:
        from .zinc15.sdf_parser import ZINC15SDFParser

        return ZINC15SDFParser
