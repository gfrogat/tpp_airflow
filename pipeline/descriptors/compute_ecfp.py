from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row

from rdkit import Chem
from rdkit.Chem import AllChem
import pandas as pd
import numpy as np

spark = (
    SparkSession.builder.appName("Process ChEMBL25 Assays")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

sc = spark.sparkContext

_data_root = "/local00/bioinf/tpp"

tox_smarts = pd.read_csv(_data_root + "/tox_smarts.txt", sep="|", header=None)
tox_smarts.columns = ["smarts"]

tox_smarts = [Chem.MolFromSmarts(tm) for tm in tox_smarts.smarts]
tox_smarts = [tm for tm in tox_smarts if tm is not None]

tox_smarts_broad = sc.broadcast(tox_smarts)


def calculate_morgan_fp(mol):
    res = np.nonzero(AllChem.GetMorganFingerprintAsBitVect(mol, 3, nBits=4096))[0]
    return res.tolist()


def calculate_tox_fp(mol):
    res = []
    for i, tm in enumerate(tox_smarts_broad.value):
        present = mol.HasSubstructMatch(tm)
        if present:
            res.append(i)
    return res


schema_tox = T.StructType(
    [
        T.StructField("tox_fp", T.ArrayType(T.IntegerType()), True),
        T.StructField("morgan_fp", T.ArrayType(T.IntegerType()), True),
    ]
)


@F.udf(returnType=schema_tox)
def calculate_descriptors(inchi):
    mol = Chem.MolFromInchi(inchi[0])

    tox_fp = None
    morgan_fp = None

    if mol is not None:
        try:
            tox_fp = calculate_tox_fp(mol)
            morgan_fp = calculate_morgan_fp(mol)
        except Exception:
            tox_fp = None
            morgan_fp = None

    row = Row(tox_fp=tox_fp, morgan_fp=morgan_fp)
    return row


df = spark.read.parquet(_data_root + "/chembl_25/chembl_25_assays_flattened.parquet")
df = df.repartition(400)
df = df.withColumn("descriptors", calculate_descriptors(F.col("inchi")))

df_descriptors = df.select("inchikey", "inchi", "activity", "index", "descriptors.*")
df_descriptors = df_descriptors.coalesce(10)
df_descriptors.write.parquet(_data_root + "/descriptors/ecfp6_tox.parquet")
