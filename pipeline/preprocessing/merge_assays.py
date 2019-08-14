from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

from pathlib import Path

spark = (
    SparkSession.builder.appName("Process ChEMBL25 Assays")
        .config("spark.sql.execution.arrow.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
)

_data_root = Path("/local00/bioinf/tpp")

# PubChem - load assays
pubchem_assays = spark.read.parquet(
    (_data_root / "pubchem_20190717/pubchem_processed.parquet").as_posix()
)

# PubChem - load compounds
pubchem_compounds = spark.read.parquet(
    (_data_root / "pubchem_20190717/pubchem_compounds.parquet").as_posix()
)

# ChEMBL - load assays
chembl_assays = spark.read.parquet(
    (_data_root / "chembl_25/chembl_25_assays_cleaned.parquet").as_posix()
)

# Reformat activity column
chembl_assays = (
    chembl_assays
        .filter(F.col("activity").isin([1, 3]))
        .withColumn("activity", F.col("activity") - 2)
)

# Only use assays that with more than 100 measurements
w = Window.partitionBy("assay_id")
chembl_assays = (
    chembl_assays.select("assay_id", "mol_id", "activity")
        .withColumn("count", F.count("*").over(w))
        .filter(F.col("count") > 100)
        .drop("count")
)

# ChEMBL - load compounds
chembl_compounds = spark.read.parquet(
    (_data_root / "chembl_25/chembl_25_compounds.parquet").as_posix()
)

# Assay IDs
pubchem_ids = (
    pubchem_assays.select("aid")
        .distinct()
        .sort(F.asc("aid"))
        .withColumn("assay_id", F.col("aid").cast(T.StringType()))
        .drop("aid")
)

chembl_ids = chembl_assays.select("assay_id").distinct().sort(F.asc("assay_id"))

assay_id_schema = T.StructType(
    [
        T.StructField("assay_id", T.StringType(), False),
        T.StructField("global_id", T.IntegerType(), False),
    ]
)

assay_ids = pubchem_ids.union(chembl_ids)
assay_ids = assay_ids.rdd.map(lambda x: x[0]).zipWithIndex().toDF(assay_id_schema)
assay_ids.write.parquet((_data_root / "merged_assay_ids.parquet").as_posix())

# Compound IDs
pubchem_compound_ids = pubchem_compounds.select("inchikey", "mol_id").sort(F.asc("mol_id"))

chembl_compound_ids = chembl_compounds.select("inchikey", "mol_id").sort(
    F.asc("mol_id")
)

compound_ids = pubchem_compound_ids.union(chembl_compound_ids)
compound_ids.write.parquet((_data_root / "merged_compound_ids.parquet").as_posix())

# Merge PubChem
pubchem_assays = pubchem_assays.alias("pa").join(
    pubchem_compounds.alias("pc"), F.col("pa.cid") == F.col("pc.mol_id")
)
pubchem_assays = pubchem_assays.withColumn("dataset", F.lit("PubChem"))
pubchem_assays = pubchem_assays.withColumn(
    "assay_id", F.col("aid").cast(T.StringType())
)

pubchem_assays = pubchem_assays.select(
    "inchikey", "assay_id", "mol_file", "activity", "dataset"
)

# Merge ChEMBL
chembl_assays = chembl_assays.alias("ca").join(
    chembl_compounds.alias("cc"), F.col("ca.mol_id") == F.col("cc.mol_id")
)
chembl_assays = chembl_assays.withColumn("dataset", F.lit("ChEMBL"))

chembl_assays = chembl_assays.select(
    "inchikey", "assay_id", "mol_file", "activity", "dataset"
)

# Merge Assays
assays = pubchem_assays.union(chembl_assays)
merged_assays = (
    assays.alias("a")
        .join(assay_ids.alias("ai"), F.col("a.assay_id") == F.col("ai.assay_id"))
        .select("inchikey", "mol_file", "global_id", "activity", "dataset")
)
merged_assays.write.parquet((_data_root / "merged_assays.parquet").as_posix())
