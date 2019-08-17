from pathlib import Path

import numpy as np
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from rdkit import Chem

_data_root = Path("/local00/bioinf/tpp")

calc = Calculator(descriptors, ignore_3D=True)


def calculate_mordred_features(mol: Chem.Mol):
    res = calc(mol)
    res = np.array(list(res)).astype(np.float)
    return Vectors.dense(res)


@F.udf(returnType=VectorUDT())
def calculate_descriptors(mol_file: str):
    mol = Chem.MolFromMolBlock(mol_file)

    mordred_features = None

    if mol is not None:
        try:
            mordred_features = calculate_mordred_features(mol)
        except Exception:
            mordred_features = None

    return mordred_features


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Compute Mordred Fingerprints") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    data = spark.read.parquet((_data_root / "merged_data_mixed.parquet").as_posix()).repartition(200)

    column_names = data.schema.names
    descriptor_names = ["descriptors.{}".format(name) for name in schema.names]
    query_names = column_names + descriptor_names

    data = data.withColumn("descriptors", calculate_descriptors(F.col("mol_file").getItem(0)))
    data_descriptors = data.select(*query_names)

    data_descriptors.write.parquet((_data_root / "tmp/test_mordred_features.parquet").as_posix())
