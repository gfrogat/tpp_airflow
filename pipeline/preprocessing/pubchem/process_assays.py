from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pathlib import Path

_data_root = Path("/local00/bioinf/tpp/pubchem_20190717")

pubchem_assay_schema = T.StructType(
    [
        T.StructField("aid", T.IntegerType(), False),
        T.StructField("cid", T.IntegerType(), False),
        T.StructField("activity_outcome", T.IntegerType(), False),
    ]
)

if __name__ == '__main__':
    try:
        spark = SparkSession \
            .builder \
            .appName("Process PubChem Assays") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        assays = spark \
            .read \
            .parquet((_data_root / "pubchem_assays.parquet").as_posix()) \
            .dropna()

        # remove assays with few measurements
        w = Window.partitionBy("aid")

        # Filter out assays that don't appear at least 25 times
        assays = assays \
            .select("aid", "cid", "activity_outcome") \
            .withColumn("count", F.count("*").over(w)) \
            .filter(F.col("count") > 25) \
            .drop("count")

        assays = assays \
            .withColumn("affinity", F.when(F.col("activity_outcome") == 3, 1.0).otherwise(0.0)) \
            .drop("activity_outcome")

        assays = assays \
            .groupby(["aid", "cid"]) \
            .agg(F.mean(F.col("affinity")).alias("avg_affinity")) \
            .filter(F.col("avg_affinity") != 0.5)

        assays = assays \
            .withColumn("activity", (2 * (F.round(F.col("avg_affinity")) - 0.5)).cast(T.IntegerType())) \
            .drop("avg_affinity")

        processed_assays = assays \
            .withColumn("actives", F.count(F.when(F.col("activity") == 1, 0)).over(w)) \
            .withColumn("inactives", F.count(F.when(F.col("activity") == -1, 0)).over(w)) \
            .filter(
                (F.col("actives") > 10)
                & (F.col("inactives") > 10)
                & (F.col("actives") + F.col("inactives") > 25)) \
            .select("cid".alias("mol_id"), "aid".alias("assay_id"), "activity")

        processed_assays \
            .write \
            .parquet((_data_root / "pubchem_assays_processed.parquet").as_posix())
    except Exception:
        # handle Exception
    finally:
        spark.stop()
