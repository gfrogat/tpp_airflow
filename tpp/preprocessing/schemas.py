import pyspark.sql.types as T

assay_id_schema = T.StructType(
    [
        T.StructField("assay_id", T.StringType(), False),
        T.StructField("dataset", T.StringType(), False),
        T.StructField("global_id", T.IntegerType(), False),
    ]
)

compound_id_schema = T.StructType(
    [
        T.StructField("inchikey", T.StringType(), False),
        T.StructField("mol_id", T.IntegerType(), False),
        T.StructField("dataset", T.StringType(), False),
    ]
)

merged_data_schema = T.StructType(
    [
        T.StructField("inchikey", T.StringType(), False),
        T.StructField("assay_id", T.StringType(), False),
        T.StructField("mol_file", T.StringType(), False),
        T.StructField("activity", T.IntegerType(), False),
        T.StructField("dataset", T.StringType(), False),
    ]
)
