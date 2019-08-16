from pathlib import Path

import numpy as np
from mordred import Calculator, descriptors
from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from rdkit import Chem
from rdkit.Chem import MACCSkeys

spark = (
    SparkSession.builder.appName("Process ChEMBL25 Assays")
        .config("spark.sql.execution.arrow.enabled", "true")
        .getOrCreate()
)

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

_data_root = Path("/local00/bioinf/tpp")

calc = Calculator(descriptors, ignore_3D=True)


def calculate_maccs_fp(mol):
    res = np.nonzero(MACCSkeys.GenMACCSKeys(mol))[0]
    return res.tolist()


def calculate_rdkit_fp(mol):
    res = np.nonzero(Chem.RDKFingerprint(mol, maxPath=6))[0]
    return res.tolist()


def calculate_mordred_features(mol):
    res = calc(mol)
    res = np.array(list(res)).astype(np.float)
    return res.tolist()


schema = T.StructType(
    [
        T.StructField("rdkit_fp", T.ArrayType(T.IntegerType()), True),
        T.StructField("maccs_fp", T.ArrayType(T.IntegerType()), True),
        T.StructField("mordred_features", T.ArrayType(T.DoubleType()), True),
    ]
)


@F.udf(returnType=schema)
def calculate_descriptors(molfile):
    mol = Chem.MolFromMolBlock(molfile)

    rdkit_fp = None
    maccs_fp = None
    mordred_features = None

    if mol is not None:
        try:
            rdkit_fp = calculate_rdkit_fp(mol)
            maccs_fp = calculate_maccs_fp(mol)
            mordred_features = calculate_mordred_features(mol)
        except Exception:
            rdkit_fp = None
            maccs_fp = None
            mordred_features = None

    row = Row(rdkit_fp=rdkit_fp, maccs_fp=maccs_fp, mordred_features=mordred_features)
    return row


@F.udf(returnType=VectorUDT())
def transform_mordred_features(array):
    return Vectors.dense(array)


df = spark.read.parquet((_data_root / "merged_data_mixed.parquet").as_posix()).repartition(200)

column_names = df.schema.names
descriptor_names = ["descriptors.{}".format(name) for name in schema.names]
query_names = column_names + descriptor_names

df = df.withColumn("descriptors", calculate_descriptors(F.col("mol_file").getItem(0)))
df_descriptors = df.select(*query_names)

df_descriptors = df_descriptors.withColumn("mordred_features_vec", transform_mordred_features(F.col("mordred_features")))
df_descriptors.write.parquet((_data_root / "tmp/test_mordred_features.parquet").as_posix())

df_descriptors = spark.read.parquet((_data_root / "tmp/test_mordred_features.parquet").as_posix())

scaler = StandardScaler(inputCol="mordred_features_vec", outputCol="mordred_features_scaled",
                        withStd=True, withMean=True)
model = scaler.fit(df_descriptors)

df_descriptors_scaled = model.transform(df_descriptors)

df_descriptors = df_descriptors \
    .drop("mordred_features", "mordred_features_vec")

df_descriptors.write.parquet((_data_root / "tmp/test_mordred_features_normalized.parquet").as_posix())
