from pathlib import Path

import numpy as np
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from rdkit import Chem
from rdkit.Chem import MACCSkeys

_data_root = Path("/local00/bioinf/tpp")



def calculate_maccs_fp(mol):
    res = np.nonzero(MACCSkeys.GenMACCSKeys(mol))[0]
    return res.tolist()


def calculate_rdkit_fp(mol):
    res = np.nonzero(Chem.RDKFingerprint(mol, maxPath=6))[0]
    return res.tolist()

schema = T.StructType(
    [
        T.StructField("rdkit_fp", T.ArrayType(T.IntegerType()), True),
        T.StructField("maccs_fp", T.ArrayType(T.IntegerType()), True),
    ]
)


@F.udf(returnType=schema)
def calculate_descriptors(molfile):
    mol = Chem.MolFromMolBlock(molfile)

    rdkit_fp = None
    maccs_fp = None

    if mol is not None:
        try:
            rdkit_fp = calculate_rdkit_fp(mol)
            maccs_fp = calculate_maccs_fp(mol)
        except Exception:
            rdkit_fp = None
            maccs_fp = None

    row = Row(rdkit_fp=rdkit_fp, maccs_fp=maccs_fp)
    return row




if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Process ChEMBL25 Assays") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    data = spark.read.parquet((_data_root / "merged_data_mixed.parquet").as_posix()).repartition(200)

    column_names = data.schema.names
    descriptor_names = ["descriptors.{}".format(name) for name in schema.names]
    query_names = column_names + descriptor_names

    data = data.withColumn("descriptors", calculate_descriptors(F.col("mol_file").getItem(0)))
    data_descriptors = data.select(*query_names)

    data_descriptors = data_descriptors.withColumn("mordred_features_vec", transform_mordred_features(F.col("mordred_features")))
    data_descriptors.write.parquet((_data_root / "tmp/test_mordred_features.parquet").as_posix())
