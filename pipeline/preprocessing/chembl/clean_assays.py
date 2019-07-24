from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = (
    SparkSession.builder.appName("Process ChEMBL25 Assays")
    .config("spark.sql.execution.arrow.enabled", "true")
    .getOrCreate()
)

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
        T.StructField("confidence_score", T.LongType(), True),
        T.StructField("activity2", T.LongType(), False),
    ]
)

# df = spark.read.parquet("/publicdata/tpp/ChEMBL/chembl_25/chembl_25_activity.parquet")
df = spark.read.parquet(
    "/local00/bioinf/tpp/chembl_25/chembl_25_activity_protocol.parquet"
)


@F.pandas_udf(T.IntegerType(), F.PandasUDFType.GROUPED_AGG)
def clean_activity_labels(activities):
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


df_processed = df.groupby(["assay_id", "mol_id"]).agg(
    clean_activity_labels(F.col("activity")).alias("activity")
)
df_processed.write.parquet(
    "/local00/bioinf/tpp/chembl_25/chembl_25_assays_cleaned.parquet"
)
