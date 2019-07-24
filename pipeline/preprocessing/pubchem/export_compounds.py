import gzip
from pathlib import Path

import pandas as pd

from rdkit import Chem, rdBase

import glob

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = (
    SparkSession.builder.appName("Export PubChem Compounds")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

sc = spark.sparkContext

log4jLogger = sc._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)

rdBase.DisableLog("rdApp.*")

used_compounds = pd.read_feather(
    "/local00/bioinf/tpp/pubchem_20190717/pubchem_compound_ids.feather"
)

used_compounds = set(used_compounds.cid)

used_compounds_broad = sc.broadcast(used_compounds)


def read_compound_info(filepath, used_compounds_broad):
    results = []

    with gzip.open(filepath, mode="rb") as sdffile:
        fsuppl = Chem.ForwardSDMolSupplier(sdffile)
        for mol in fsuppl:
            if mol is not None:
                try:
                    props = mol.GetPropsAsDict()
                    cid = props["PUBCHEM_COMPOUND_CID"]
                    smiles = props["PUBCHEM_OPENEYE_CAN_SMILES"]
                    inchikey = props["PUBCHEM_IUPAC_INCHIKEY"]
                    inchi = props["PUBCHEM_IUPAC_INCHI"]

                    if cid in used_compounds_broad.value:
                        triple = Row(
                            cid=cid, smiles=smiles, inchikey=inchikey, inchi=inchi
                        )
                        results.append(triple)
                except Exception:
                    pass

    return results


pubchem_compounds = Path("/local00/bioinf/tpp/pubchem_20190717")
dir_structure = Path("ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/")

path_sdffiles = pubchem_compounds / dir_structure
files = glob.glob(str(path_sdffiles) + "/*.gz")

files = sc.parallelize(files, 200)

result = files.flatMap(lambda j: read_compound_info(j, used_compounds_broad))
result = result.toDF()
result.write.parquet("/local00/bioinf/tpp/pubchem_20190717/pubchem_compounds.parquet")
