from pathlib import Path

# from pyspark import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
import numpy as np


_data_root = Path("/local00/bioinf/tpp")


@F.udf(T.ArrayType(T.DoubleType()))
def cast_to_array(vec):
    return vec.values.tolist()


@F.pandas_udf(T.DoubleType(), F.PandasUDFType.GROUPED_AGG)
def median(vec):
    vec = np.asarray(vec)
    median = np.median(vec)
    return median


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Process ChEMBL25 Assays")
        .config("spark.sql.execution.arrow.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    df_descriptors = spark.read.parquet(
        (_data_root / "tmp/test_mordred_features.parquet").as_posix()
    )

    mordred_exploded = df_descriptors.select(
        F.col("inchikey"), F.posexplode("mordred_features")
    )

    non_nan = mordred_exploded.filter(F.isnan("col") == False)

    nan = mordred_exploded.filter(F.isnan("col") == True)

    w = Window.partitionBy("pos")

    # Standardize non-nan features
    non_nan = non_nan.withColumn("mean", F.mean("col").over(w)).withColumn(
        "std", F.stddev("col").over(w)
    )
    non_nan = non_nan.withColumn("norm", (F.col("col") - F.col("mean")) / F.col("std"))

    # Compute median of standardized features
    norm_median = non_nan.withColumn("norm_median", median("norm").over(w)).select(
        "pos", "norm_median"
    )

    # Impute missing values with standardized median
    nan_imputed = (
        nan.alias("nan")
        .join(norm_median.alias("nm"), F.col("nan.pos") == F.col("nm.pos"))
        .select(F.col("inchikey"), F.col("nan.pos"), F.col("norm_median").alias("norm"))
    )

    # merge datasets again
    features = non_nan.select("inchikey", "pos", "norm").union(nan_imputed)

    features.write.parquet(
        (_data_root / "tmp/test_mordred_features_normalized.parquet").as_posix()
    )

    # Pandas UDF median  :  groupby --> median
    # join pos median to nan dataframe

    # join both dataframes union
    # tuple oven index, col
    # array from stuff _ sort _array
    # extract array values

    scaler = StandardScaler(
        inputCol="mordred_features_imputed",
        outputCol="mordred_features_scaled",
        withStd=True,
        withMean=True,
    )
    model = scaler.fit(df_descriptors)

    df_descriptors_scaled = model.transform(df_descriptors)

    df_descriptors = df_descriptors_scaled.drop(
        "mordred_features", "mordred_features_vec"
    )

    df_descriptors_scaled.select(
        cast_to_array(F.col("mordred_features_scaled")).alias("mordred_features")
    )

    df_descriptors.write.parquet(
        (_data_root / "tmp/test_mordred_features_normalized.parquet").as_posix()
    )
