from pyspark.sql import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T

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
    features = (
        features.withColumn("count", F.count("*").over(w))
        .filter(F.col("count") > feature_freq)
        .drop("count")
    )

    features = features.withColumn("mean", F.avg("value").over(w)).withColumn(
        "std", F.stddev("value").over(w)
    )

    feature_ids = (
        features.select("key", "mean", "std")
        .dropDuplicates(["key"])
        .rdd.zipWithIndex()
        .map(lambda x: (*x[0], x[1]))
        .toDF(schema)
    )

    # Normalize feature frequencies
    # if a feature has stddev 0 --> replace value by 1.0
    features = (
        features.withColumn(
            "z_value",
            F.when(
                F.col("std") != 0.0, (F.col("value") - F.col("mean")) / F.col("std")
            ).otherwise(F.lit(1.0)),
        )
        .drop("value", "mean", "std")
        .select("inchikey", "key", "z_value")
    )

    features = features.alias("f").join(
        feature_ids.alias("fi"), F.col("f.key") == F.col("fi.key")
    )

    features = features.select("inchikey", "id", "z_value")

    features = features.groupBy("inchikey").agg(
        F.map_from_entries(
            F.sort_array(F.collect_list(F.struct(F.col("id"), F.col("z_value"))))
        ).alias("{}_clean".format(feature_name))
    )

    return features, feature_ids
