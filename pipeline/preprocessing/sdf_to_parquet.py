import argparse
import gzip
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from rdkit import Chem

_data_root = Path("/local00/bioinf/tpp")

schema = T.StructType(
    [
        T.StructField("mol_id", T.StringType(), False),
        T.StructField("inchikey", T.StringType(), False),
        T.StructField("mol_file", T.StringType(), False),
    ]
)

zinc15_schema = T.StructType(
    [
        T.StructField("mol_id", T.StringType(), False),
        T.StructField("inchikey", T.StringType(), False),
        T.StructField("mol_file", T.StringType(), False),
        T.StructField("gene_name", T.StringType(), False),
        T.StructField("affinity", T.DoubleType(), False),
    ]
)


def export_molecules_chembl(sdf_path: Path):
    res = []

    suppl = Chem.SDMolSupplier(sdf_path.as_posix())

    for mol in suppl:
        if mol is not None:
            try:
                chembl_id = mol.GetProp("chembl_id")
                inchikey = Chem.MolToInchiKey(mol)
                mol_block = Chem.MolToMolBlock(mol)
                row = Row(mol_id=chembl_id, inchikey=inchikey, mol_file=mol_block)
                res.append(row)
            except Exception:
                pass

    return res


def export_molecules_pubchem(sdf_path: Path):
    res = []

    with gzip.open(sdf_path.as_posix(), "rb") as f:
        suppl = Chem.ForwardSDMolSupplier(f)
        for mol in suppl:
            if mol is not None:
                try:
                    cid = mol.GetProp("PUBCHEM_COMPOUND_CID")
                    inchikey = Chem.MolToInchiKey(mol)
                    mol_block = Chem.MolToMolBlock(mol)
                    row = Row(mol_id=cid, inchikey=inchikey, mol_file=mol_block)
                    res.append(row)
                except Exception:
                    pass

    return res


def export_molecules_zinc15(sdf_path: Path):
    res = []

    suppl = Chem.SDMolSupplier(sdf_path.as_posix())

    for mol in suppl:
        if mol is not None:
            try:
                zinc_id = mol.GetProp("zinc_id")
                inchikey = Chem.MolToInchiKey(mol)
                mol_block = Chem.MolToMolBlock(mol)
                gene_name = mol.GetProp("gene_name")
                affinity = float(mol.GetProp("affinity"))
                row = Row(mol_id=zinc_id, inchikey=inchikey, mol_file=mol_block, gene_name=gene_name, affinity=affinity)
                res.append(row)
            except Exception:
                pass

    return res


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Export SDF files to parquet')
    parser.add_argument('--dataset', type=str, help="dataset to expord sdf files", default="ChEMBL")

    args = parser.parse_args()

    if args.dataset == "ChEMBL":
        _sdf_root = _data_root / "chembl_25/chembl_25_shards"
        sdf_path = _sdf_root.glob("*.sdf")
        parquet_path = _data_root / "chembl_25/chembl_25_compounds.parquet"
        export_molecules = export_molecules_chembl
    elif args.dataset == "PubChem":
        _sdf_root = _data_root / "pubchem_20190717/ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/"
        sdf_path = _sdf_root.glob("*.sdf.gz")
        parquet_path = _data_root / "pubchem_20190717/pubchem_compounds.parquet"
        export_molecules = export_molecules_pubchem
    elif args.dataset == "ZINC15":
        _sdf_root = _data_root / "ZINC15/sdf"
        sdf_path = _sdf_root.glob("*.sdf")
        parquet_path = _data_root / "ZINC15/zinc15_data_full.parquet"
        export_molecules = export_molecules_zinc15
        schema = zinc15_schema
    else:
        raise ValueError("Unsupported dataset")

    try:
        spark = SparkSession \
            .builder \
            .appName("Process ChEMBL25 Assays") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        sc = spark.sparkContext

        sdf_files = list(sdf_path)
        sdf_files = sc \
            .parallelize(list(sdf_path)) \
            .repartition(200)

        sdf_parquet = sdf_files \
            .flatMap(export_molecules) \
            .toDF(schema=schema)

        sdf_parquet \
            .write \
            .parquet(parquet_path.as_posix())
    except Exception:
        # handle exception
    finally:
        spark.stop()
