from pathlib import Path

from pyspark import SparkSession
from pyspark.ml.feature import StandardScaler

_data_root = Path("/local00/bioinf/tpp")

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

    scaler = StandardScaler(
        inputCol="mordred_features_vec",
        outputCol="mordred_features_scaled",
        withStd=True,
        withMean=True,
    )
    model = scaler.fit(df_descriptors)

    df_descriptors_scaled = model.transform(df_descriptors)

    df_descriptors = df_descriptors.drop("mordred_features", "mordred_features_vec")

    df_descriptors.write.parquet(
        (_data_root / "tmp/test_mordred_features_normalized.parquet").as_posix()
    )
