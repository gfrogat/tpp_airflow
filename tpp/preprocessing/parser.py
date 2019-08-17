from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

import pyspark.sql.types as T
from pyspark.sql import Row


class SDFParser(ABC):

    @staticmethod
    @abstractmethod
    def get_schema() -> T.StructType:
        pass

    @staticmethod
    @abstractmethod
    def parse_sdf(sdf_path: Path) -> List[Row]:
        pass


class AssayParser(ABC):

    @staticmethod
    @abstractmethod
    def parse_assay(filepath: Path) -> List[Row]:
        pass
