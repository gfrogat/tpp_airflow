from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession.builder.appName("Process ChEMBL25 Assays")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

_data_root = "/publicdata/tpp/ChEMBL"

chembl25_schema = T.StructType(
    [
        T.StructField("mol_id", T.StringType(), False),
        T.StructField("assay_id", T.StringType(), False),
        T.StructField("standard_relation", T.StringType(), True),
        T.StructField("standard_value", T.DoubleType(), True),
        T.StructField("standard_units", T.StringType(), True),
        T.StructField("standard_type", T.StringType(), True),
        T.StructField("activity_comment", T.StringType(), True),
        T.StructField("doc_id", T.LongType(), True),
        T.StructField("tid", T.LongType(), True),
        T.StructField("parent_type", T.StringType(), True),
        T.StructField("target_type", T.StringType(), True),
        T.StructField("confidence_score", T.IntegerType(), True),
    ]
)

# df = spark.read.parquet(_data_root + "/chembl_25/chembl_25_assays_dump.parquet")
df = spark.read.parquet("/local00/bioinf/tpp/chembl_25/chembl_25_assays_dump.parquet")
df = df.repartition(100)

inactive_comments = set(
    [
        (
            "Not Active (inhibition < 50% @ 10 uM "
            "and thus dose-reponse curve not measured)"
        ),
        "Not Active",
        "inactive",
    ]
)
inactive_relations = set([">", ">=", "=", "~"])

active_comments = set(["active", "Active"])
active_relations = set(["<", "<=", "=", "~"])

relations = set([">", ">=", "<", "<=", "=", "~"])


@F.udf(returnType=T.IntegerType())
def generate_activity_labels(
    activity_comment, standard_value, standard_units, standard_relation
):
    if activity_comment in inactive_comments:
        return 1  # inactive
    elif activity_comment in active_comments:
        return 3  # active
    elif (
        standard_value is None
        or standard_units != "nM"
        or standard_relation not in relations
    ):
        return 0  # unspecified
    else:
        if (
            standard_value <= 10 ** (9.0 - 5.5)
            and standard_relation in active_relations
        ):
            return 3  # active
        elif (
            standard_value < 10 ** (9.0 - 5.0) and standard_relation in active_relations
        ):
            return 13  # active (weakly)
        elif (
            standard_value >= 10 ** (9.0 - 4.5)
            and standard_relation in inactive_relations
        ):
            return 1  # inactive
        elif (
            standard_value > 10 ** (9.0 - 5.0)
            and standard_relation in inactive_relations
        ):
            return 11  # inactive (weakly)
        elif standard_value > 10 ** (9.0 - 5.5) and standard_value < 10 ** (9.0 - 4.5):
            return 2  # indeterminate
        else:
            return 0  # unspecified


activity_df = df.withColumn(
    "activity",
    generate_activity_labels(
        F.col("activity_comment"),
        F.col("standard_value"),
        F.col("standard_units"),
        F.col("standard_relation"),
    ),
)
# activity_df.write.parquet("/publicdata/tpp/ChEMBL/chembl_25/chembl_25_activity.parquet")
activity_df.write.parquet(
    "/local00/bioinf/tpp/chembl_25/chembl_25_activity_protocol.parquet"
)
