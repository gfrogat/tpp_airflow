from enum import Enum

from .tox_calculator import ToxCalculator

__all__ = ["SemiSparseCalculator", "ToxCalculator"]


class FeatureType(Enum):
    SEMISPARSE = "semisparse"
    TOX = "tox"
    STATIC = "static"
    MORGAN = "morgan"

    def __str__(self) -> str:
        return self.value


def get_feature_calculator(feature_type: FeatureType):

    if feature_type == FeatureType.SEMISPARSE:
        from .semisparse_calculator import SemiSparseCalculator

        return SemiSparseCalculator
    elif feature_type == FeatureType.TOX:
        from .tox_calculator import ToxCalculator

        return ToxCalculator
    elif feature_type == FeatureType.MORDRED:
        from .static_calculator import StaticCalculator

        return StaticCalculator
    elif feature_type == FeatureType.MORGAN:
        from .morgan_calculator import MorganCalculator

        return MorganCalculator
