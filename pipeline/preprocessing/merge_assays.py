from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession.builder.appName("Process ChEMBL25 Assays")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

_data_root = "/local00/bioinf/tpp"

# PubChem
pubchem_assays = spark.read.parquet(
    _data_root + "/pubchem_20190717/pubchem_processed.parquet"
)

pubchem_compounds_schema = T.StructType(
    [
        T.StructField("cid", T.IntegerType(), False),
        T.StructField("inchi", T.StringType(), False),
        T.StructField("inchikey", T.StringType(), False),
        T.StructField("smiles", T.StringType(), False),
    ]
)

pubchem_compounds = spark.read.parquet(
    _data_root + "/pubchem_20190717/pubchem_compounds.parquet"
)

# ChEMBL
chembl_assays = spark.read.parquet(_data_root + "/chembl_25/chembl_25_assays.parquet")
chembl_assays = chembl_assays.filter(F.col("activity").isin([1, 3])).withColumn(
    "activity", F.col("activity") - 2
)

chembl_compounds_schema = T.StructType(
    [
        T.StructField("mol_id", T.StringType(), False),
        T.StructField("inchi", T.StringType(), False),
        T.StructField("inchikey", T.StringType(), False),
        T.StructField("smiles", T.StringType(), False),
    ]
)

chembl_compounds = spark.read.parquet(
    _data_root + "/chembl_25/chembl_25_compounds.parquet"
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
assay_ids.write.parquet(_data_root + "/merged_assay_ids.parquet")


# Compound IDs
pubchem_compound_ids = pubchem_compounds.select("inchikey", "cid").sort(F.asc("cid"))
pubchem_compound_ids = pubchem_compound_ids.withColumn(
    "mol_id", F.col("cid").cast(T.StringType())
).drop("cid")

chembl_compound_ids = chembl_compounds.select("inchikey", "mol_id").sort(
    F.asc("mol_id")
)

compound_ids = pubchem_compound_ids.union(chembl_compound_ids)
compound_ids.write.parquet(_data_root + "/merged_compound_ids.parquet")


# Merge PubChem
pubchem_assays = pubchem_assays.alias("pa").join(
    pubchem_compounds.alias("pc"), F.col("pa.cid") == F.col("pc.cid")
)
pubchem_assays = pubchem_assays.withColumn("dataset", F.lit("PubChem"))
pubchem_assays = pubchem_assays.withColumn(
    "assay_id", F.col("aid").cast(T.StringType())
)

pubchem_assays = pubchem_assays.select(
    "inchikey", "assay_id", "inchi", "activity", "dataset"
)


# Merge ChEMBL
chembl_assays = chembl_assays.alias("ca").join(
    chembl_compounds.alias("cc"), F.col("ca.mol_id") == F.col("cc.mol_id")
)
chembl_assays = chembl_assays.withColumn("dataset", F.lit("ChEMBL"))

chembl_assays = chembl_assays.select(
    "inchikey", "assay_id", "inchi", "activity", "dataset"
)


# Merge Assays
assays = pubchem_assays.union(chembl_assays)
merged_assays = (
    assays.alias("a")
    .join(assay_ids.alias("ai"), F.col("a.assay_id") == F.col("ai.assay_id"))
    .select("inchikey", "inchi", "activity", "global_id")
)
merged_assays.write.parquet(_data_root + "/merged_assays.parquet")
