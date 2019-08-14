from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window

from pathlib import Path

spark = (
    SparkSession.builder.appName("Process ChEMBL25 Assays")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

_data_root = Path("/local00/bioinf/tpp")


# ChEMBL
chembl_assays = spark.read.parquet(
    (_data_root / "chembl_25/chembl_25_assays_cleaned.parquet").as_posix()
)
chembl_assays = chembl_assays.filter(F.col("activity").isin([1, 3])).withColumn(
    "activity", F.col("activity") - 2
)

w = Window.partitionBy("assay_id")

chembl_assays = (
    chembl_assays.select("assay_id", "mol_id", "activity")
    .withColumn("count", F.count("*").over(w))
    .filter(F.col("count") > 100)
    .drop("count")
)

chembl_compounds = spark.read.parquet(
    (_data_root / "chembl_25/chembl_25_compounds.parquet").as_posix()
)

chembl_compounds_dump = spark.read.parquet(
    (_data_root / "chembl_25/chembl_25_compounds_dump.parquet").as_posix()
)

chembl_compounds = (
    chembl_compounds.alias("cc")
    .join(chembl_compounds_dump.alias("ccd"), F.col("cc.mol_id") == F.col("ccd.mol_id"))
    .select("cc.mol_id", "mol_file", "inchi", "inchikey", "smiles")
)


chembl_ids = chembl_assays.select("assay_id").distinct().sort(F.asc("assay_id"))

assay_id_schema = T.StructType(
    [
        T.StructField("assay_id", T.StringType(), False),
        T.StructField("global_id", T.IntegerType(), False),
    ]
)

assay_ids = chembl_ids
assay_ids = assay_ids.rdd.map(lambda x: x[0]).zipWithIndex().toDF(assay_id_schema)
assay_ids.write.parquet(
    (_data_root / "chembl_25/chembl_25_assay_ids.parquet").as_posix()
)


chembl_assays = chembl_assays.alias("ca").join(
    chembl_compounds.alias("cc"), F.col("ca.mol_id") == F.col("cc.mol_id")
)
chembl_assays = chembl_assays.withColumn("dataset", F.lit("ChEMBL"))

compound_ids = chembl_assays.select("inchikey", "ca.mol_id")
compound_ids = compound_ids.dropDuplicates()
compound_ids.write.parquet(
    (_data_root / "chembl_25/chembl_25_compound_ids.parquet").as_posix()
)


chembl_assays = chembl_assays.select(
    "inchikey", "assay_id", "mol_file", "activity", "dataset"
)

assays = chembl_assays
processed_assays = (
    assays.alias("a")
    .join(assay_ids.alias("ai"), F.col("a.assay_id") == F.col("ai.assay_id"))
    .select("inchikey", "mol_file", "activity", "global_id")
)

processed_assays.write.parquet(
    (_data_root / "chembl_25/chembl_25_assays_formatted.parquet").as_posix()
)
