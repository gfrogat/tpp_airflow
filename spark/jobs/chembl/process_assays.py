from pathlib import Path
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

_data_root = Path("/local00/bioinf/tpp/ChEMBL")

active_comments = {"active", "Active"}
inactive_comments = {
    "Not Active (inhibition < 50% @ 10 uM and thus dose-response curve not measured)",
    "Not Active",
    "inactive"
}

relations = {">", ">=", "<", "<=", "=", "~"}
inactive_relations = {">", ">=", "=", "~"}
active_relations = {"<", "<=", "=", "~"}


@F.typed_udf(returnType=T.IntegerType())
def generate_activity_labels(
        activity_comment: str, standard_value: float, standard_units: str, standard_relation: str
):
    if activity_comment in inactive_comments:
        return 1  # inactive
    elif activity_comment in active_comments:
        return 3  # active
    elif standard_value is None or standard_units != "nM" or standard_relation not in relations:
        return 0  # unspecified
    else:
        if standard_value <= 10 ** (9.0 - 5.5) and standard_relation in active_relations:
            return 3  # active
        elif standard_value < 10 ** (9.0 - 5.0) and standard_relation in active_relations:
            return 13  # active (weakly)
        elif standard_value >= 10 ** (9.0 - 4.5) and standard_relation in inactive_relations:
            return 1  # inactive
        elif standard_value > 10 ** (9.0 - 5.0) and standard_relation in inactive_relations:
            return 11  # inactive (weakly)
        elif 10 ** (9.0 - 5.5) < standard_value < 10 ** (9.0 - 4.5):
            return 2  # indeterminate
        else:
            return 0  # unspecified


@F.pandas_udf(T.IntegerType(), F.PandasUDFType.GROUPED_AGG)
def clean_activity_labels(activities: List[int]):
    activities = set(activities)

    if len(activities) == 1:
        return list(activities)[0]

    is_garbage = 0 in activities
    is_inactive = 1 in activities
    is_inconclusive = 2 in activities
    is_active = 3 in activities

    if is_garbage or (is_inactive and is_active):  # garbage
        return 0
    elif is_inconclusive:  # inconclusive
        return 2
    elif is_inactive:  # inactive
        return 1
    elif is_active:  # active
        return 3

    # compute weak activity
    is_weakly_active = 11 in activities
    is_weakly_inactive = 13 in activities
    if is_weakly_inactive and is_weakly_active:  # weakly garbage
        return 10
    elif is_weakly_inactive:  # weakly inactive
        return 11
    elif is_weakly_active:  # weakly active
        return 13


if __name__ == "__main__":
    try:
        spark = SparkSession \
            .builder \
            .appName("Process ChEMBL25 Assays") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        df = spark \
            .read \
            .parquet((_data_root / "chembl_25/chembl_25_assays_dump.parquet").as_posix()) \
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
            .parquet((_data_root / "chembl_25/chembl_25_activity_cleaned.parquet").as_posix())
    except Exception:
    # handle exception
    finally:
        spark.stop()
