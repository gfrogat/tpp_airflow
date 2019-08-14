from pyspark.sql import SparkSession
import pyspark.sql.types as T
import pyspark.sql.functions as F

from pathlib import Path

spark = (
    SparkSession.builder.appName("Process ChEMBL25 Assays")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

_data_root = Path("/local00/bioinf/tpp")

df = spark.read.parquet(
    (_data_root / "chembl_25/chembl_25_semisparse.parquet/").as_posix()
)

df_test = df.select("inchikey", F.explode("ECFP"))


ecfp_ids_schema = T.StructType(
    [
        T.StructField("ecfp", T.StringType(), False),
        T.StructField("id", T.IntegerType(), False),
    ]
)

ecfp_frequencies = (
    df.select(F.explode("ECFP").alias("ECFP"))
    .select(F.trim(F.col("ECFP")).alias("ECFP"))
    .groupby("ECFP")
    .count()
)


ecfp_ids = (
    ecfp_frequencies.filter(F.col("count") > 25)
    .select("ECFP")
    .rdd.map(lambda x: x[0])
    .zipWithIndex()
    .toDF(ecfp_ids_schema)
)


tmp = (
    df_test.alias("ta")
    .join(ecfp_ids.alias("tb"), F.col("ta.col") == F.col("tb.ECFP"))
    .select("inchikey", "id")
)
df_ecfp = tmp.groupby("inchikey").agg(
    F.sort_array(F.collect_set("id")).alias("ecfp_ids")
)
df_ecfp.write.parquet((_data_root / "tmp/ecfp_formatted.parquet").as_posix())


df_ecfp_test = spark.read.parquet(
    (_data_root / "tmp/ecfp_formatted.parquet").as_posix()
)
