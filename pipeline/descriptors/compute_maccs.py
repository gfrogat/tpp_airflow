import numpy as np
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

sc = spark.sparkContext

_data_root = "/local00/bioinf/tpp"


def calculate_maccs_fp(mol):
    res = np.nonzero(MACCSkeys.GenMACCSKeys(mol))[0]
    return res.tolist()


def calculate_rdkit_fp(mol):
    res = np.nonzero(Chem.RDKFingerprint(mol, maxPath=6))[0]
    return res.tolist()


schema_tox = T.StructType(
    [
        T.StructField("rdkit_fp", T.ArrayType(T.IntegerType()), True),
        T.StructField("maccs_fp", T.ArrayType(T.IntegerType()), True),
    ]
)


@F.udf(returnType=schema_tox)
def calculate_descriptors(inchi):
    mol = Chem.MolFromInchi(inchi[0])

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


df = spark.read.parquet(_data_root + "/chembl_25/chembl_25_assays_flattened.parquet")
df = df.repartition(400)
df = df.withColumn("descriptors", calculate_descriptors(F.col("inchi")))

df_descriptors = df.select("inchikey", "inchi", "activity", "index", "descriptors.*")
df_descriptors = df_descriptors.coalesce(10)
df_descriptors.write.parquet(_data_root + "/descriptors/maccs_rdkit.parquet")
