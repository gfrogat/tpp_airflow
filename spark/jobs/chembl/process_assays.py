from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

from tpp.preprocessing.chembl import clean_activity_labels, \
    generate_activity_labels

_data_root = Path("/local00/bioinf/tpp/ChEMBL")

if __name__ == "__main__":

    generate_activity_labels = F.udf(generate_activity_labels, T.IntegerType())
    clean_activity_labels = F.pandas_udf(clean_activity_labels,
                                         T.IntegerType(),
                                         F.PandasUDFType.GROUPED_AGG)

    try:
        spark = SparkSession \
            .builder \
            .appName("Process ChEMBL25 Assays") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        df = spark \
            .read \
            .parquet((
                             _data_root / "chembl_25/chembl_25_assays_dump.parquet").as_posix()) \
            .repartition(100)

        df_processed = df \
            .withColumn("activity",
                        generate_activity_labels(
                            F.col("activity_comment"),
                            F.col("standard_value"),
                            F.col("standard_units"),
                            F.col("standard_relation")))

        df_cleaned = df_processed \
            .groupby(["assay_id", "mol_id"]) \
            .agg(clean_activity_labels(F.col("activity")).alias("activity"))

        df_cleaned \
            .write \
            .parquet((
                             _data_root / "chembl_25/chembl_25_activity_cleaned.parquet").as_posix())
    except Exception:
    # handle exception
    finally:
        spark.stop()
