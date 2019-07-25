from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row

from rdkit import Chem
from rdkit.Chem import AllChem
from pathlib import Path
import gzip

spark = (
    SparkSession.builder.appName("Process ChEMBL25 Assays")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

sc = spark.sparkContext


def export_molecules_chembl(sdf_file):
    suppl = Chem.SDMolSupplier(sdf_file.as_posix())

    res = []

    for mol in suppl:
        if mol is not None:
            try:
                mol_block = Chem.MolToMolBlock(mol)
                chembl_id = mol.GetProp("chembl_id")
                row = Row(mol_id=chembl_id, mol_file=mol_block)
                res.append(row)
            except Exception:
                pass

    return res


def export_molecules_pubchem(sdf_file):
    res = []

    with gzip.open(sdf_file.as_posix(), "rb") as f:
        suppl = Chem.ForwardSDMolSupplier(f)
        for mol in suppl:
            if mol is not None:
                try:
                    mol_block = Chem.MolToMolBlock(mol)
                    cid = mol.GetProp("PUBCHEM_COMPOUND_CID")
                    row = Row(mol_id=cid, mol_file=mol_block)
                    res.append(row)
                except Exception:
                    pass

    return res


schema = T.StructType(
    [
        T.StructField("mol_id", T.StringType(), False),
        T.StructField("mol_file", T.StringType(), False),
    ]
)

"""
# _data_root = Path("/local00/bioinf/tpp")
_data_root = Path("/data/ChEMBL/")
sdf_path = _data_root.glob("chembl_25/chembl_25_shards/*.sdf")
parquet_path = _data_root / "chembl_25/chembl_25_compounds.parquet"
sdf_files = sc.parallelize(list(sdf_path))
sdf_parquet = sdf_files.flatMap(export_molecules_chembl).toDF(schema=schema)
"""

# _data_root = Path("/data/PubChem")
_data_root = Path("/local00/bioinf/tpp/pubchem_20190717")
_sdf_root = _data_root / "ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/"
sdf_path = _sdf_root.glob("*.sdf.gz")
parquet_path = _data_root / "pubchem_compounds_full.parquet"

sdf_files = sc.parallelize(list(sdf_path))
sdf_files = sdf_files.repartition(200)
sdf_parquet = sdf_files.flatMap(export_molecules_pubchem).toDF(schema=schema)

sdf_parquet.write.parquet(parquet_path.as_posix())
