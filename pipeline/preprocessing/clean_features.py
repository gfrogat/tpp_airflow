from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

_data_root = Path("/local00/bioinf/tpp")

schema = T.StructType(
    [
        T.StructField("key", T.StringType(), False),
        T.StructField("mean", T.DoubleType(), False),
        T.StructField("std", T.DoubleType(), False),
        T.StructField("id", T.IntegerType(), False),
    ]
)


def clean_frequency_feature(df, feature_name, feature_freq=10):
    features = df.select("inchikey", F.explode(feature_name))

    w = Window.partitionBy("key")
    features = features \
        .withColumn("count", F.count("*").over(w)) \
        .filter(F.col("count") > feature_freq) \
        .drop("count")

    features = features \
        .withColumn("mean", F.avg("value").over(w)) \
        .withColumn("std", F.stddev("value").over(w))

    feature_ids = features \
        .select("key", "mean", "std") \
        .dropDuplicates(["key"]) \
        .rdd \
        .zipWithIndex() \
        .map(lambda x: (*x[0], x[1])) \
        .toDF(schema)

    feature_ids.write.parquet(
        (_data_root / "merged_data_mixed_features_{}_ids.parquet".format(feature_name)).as_posix())

    # Normalize feature frequencies
    # if a feature has stddev 0 --> replace value by 1.0
    features = features \
        .withColumn("z_value",
                    F.when(F.col("std") != 0.0, (F.col("value") - F.col("mean")) / F.col("std")).otherwise(F.lit(1.0))) \
        .drop("value", "mean", "std") \
        .select("inchikey", "key", "z_value")

    features = features.alias("f") \
        .join(feature_ids.alias("fi"), F.col("f.key") == F.col("fi.key"))

    features = features \
        .select("inchikey", "id", "z_value")

    features = features \
        .groupBy("inchikey") \
        .agg(F.map_from_entries(F.sort_array(F.collect_list(F.struct(F.col("id"), F.col("z_value"))))).alias(
        "{}_clean".format(feature_name)))

    return features


def clean_feature(df, feature_name):
    clean_features = clean_frequency_feature(df, feature_name)
    df = df.alias("df") \
        .join(clean_features.alias("clean"), F.col("df.inchikey") == F.col("clean.inchikey")) \
        .select("df.*", "clean.{}_clean".format(feature_name)) \
        .drop(feature_name)

    return df


if __name__ == '__main__':
    feature_list = ["ECFC4", "DFS8", "ECFC6", "CATS2D", "SHED"]

    try:
        spark = SparkSession \
            .builder \
            .appName("Clean frequency features") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.executor.memory", "5g") \
            .getOrCreate()

        data_mixed = spark.read.parquet((_data_root / "merged_data_mixed.parquet").as_posix())

        for idx, feature_name in enumerate(feature_list):
            data_mixed = clean_feature(data_mixed, feature_name)
            data_mixed.write.parquet((_data_root / "tmp/merged_data_mixed_chkpt{}.parquet".format(idx)).as_posix())
            data_mixed = spark.read.parquet(
                (_data_root / "tmp/merged_data_mixed_chkpt{}.parquet".format(idx)).as_posix())

        data_mixed.write.parquet((_data_root / "merged_data_mixed_clean.parquet").as_posix())
    except Exception:
    # handle Exception
    finally:
        spark.stop()
