import pyspark.sql.types as T

assay_id_schema = T.StructType(
    [
        T.StructField("assay_id", T.StringType(), False),
        T.StructField("dataset", T.StringType(), False),
        T.StructField("global_id", T.IntegerType(), False),
    ]
)
