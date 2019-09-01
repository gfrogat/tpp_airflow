import argparse
from pathlib import Path

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from tpp.preprocessing.chembl import clean_activity_labels, generate_activity_labels
from tpp.utils.argcheck import check_input_path, check_output_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="PySpark ChEMBL Assay Processing",
        description="Process ChEMBL assay data from SQLite dump to `parquet` format",
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        metavar="PATH",
        dest="input_path",
        help=f"Path to PubChem folder (FTP schema)",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        metavar="PATH",
        dest="output_path",
        help="Path where output should be written to in `parquet` format",
    )
    parser.add_argument(
        "--num-partitions", type=int, dest="num_partitions", default=100
    )

    args = parser.parse_args()

    check_input_path(args.input_path)
    check_output_path(args.output_path)

    generate_activity_labels = F.udf(generate_activity_labels, T.IntegerType())
    clean_activity_labels = F.pandas_udf(
        clean_activity_labels, T.IntegerType(), F.PandasUDFType.GROUPED_AGG
    )

    try:
        spark = (
            SparkSession.builder.appName("Process ChEMBL25 Assays")
            .config("spark.sql.execution.arrow.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        df = spark.read.parquet(args.input_path.as_posix()).repartition(
            parser.num_partitions
        )

        df_processed = df.withColumn(
            "activity",
            generate_activity_labels(
                F.col("activity_comment"),
                F.col("standard_value"),
                F.col("standard_units"),
                F.col("standard_relation"),
            ),
        )

        df_cleaned = df_processed.groupby(["assay_id", "mol_id"]).agg(
            clean_activity_labels(F.col("activity")).alias("activity")
        )

        df_cleaned.write.parquet(args.output_path.as_posix())
    except Exception:
        # handle exception
        pass
    finally:
        spark.stop()
