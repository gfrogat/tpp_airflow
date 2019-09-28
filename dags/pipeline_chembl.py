import datetime as dt

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "tpp",
    "depends_on_past": False,
    "start_date": dt.datetime(2019, 8, 1),
    "email": ["tpp@ml.jku.at"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=30),
}

dag_config = Variable.get("chembl_config", deserialize_json=True)

with DAG(
    "pipeline_chembl",
    default_args=default_args,
    schedule_interval=None,
    params=dag_config,
) as dag:

    # Create Folder Structure
    create_folder_structure_command = """
    mkdir -p {{ params.tpp_root }}/{{ run_id }}/{chembl,tmp}
    """
    create_folder_structure = BashOperator(
        task_id="create_folder_structure", bash_command=create_folder_structure_command
    )

    # Download ChEMBL data
    download_chembl_command = """
    bash {{ params.tpp_python_home }}/scripts/download_chembl.sh
    """
    download_chembl = BashOperator(
        task_id="download_chembl",
        bash_command=download_chembl_command,
        env={
            "CHEMBL_DIR": "{{ params.tpp_root }}/{{ run_id }}/chembl/data",
            "RELEASE": "{{ params.chembl_release }}",
        },
    )

    # Extract downloaded ChEMBL data
    extract_chembl_command = """
    pushd ${CHEMBL_RELEASE_DIR} && \
    gunzip chembl_*.sdf.gz && \
    tar --strip-components=1 -zxf chembl_*_sqlite.tar.gz && \
    popd
    """
    extract_chembl = BashOperator(
        task_id="extract_chembl",
        bash_command=extract_chembl_command,
        env={
            "CHEMBL_RELEASE_DIR": (
                "{{ params.tpp_root }}/{{ run_id }}/chembl/data/"
                "chembl_{{ params.chembl_release }}"
            )
        },
    )

    # Export SQLite Assays
    export_sqlite_command = """
    python {{ params.tpp_python_home }}/scripts/export_chembl_sqlite.py \
        --input {{ params.tpp_root }}/{{ run_id }}/{{ params.chembl_sqlite_path}} \
        --output {{ params.tpp_root }}/{{ run_id }}/{{ params.output_path }} \
        --export {{ params.export_type }}
    """
    export_sqlite_assays = BashOperator(
        task_id="export_sqlite_assays",
        bash_command=export_sqlite_command,
        params={"output_path": "chembl/assays.parquet", "export_type": "assays"},
    )

    # Shard SDF
    shard_sdf_command = """
    python {{ params.tpp_python_home }}/scripts/shard_sdf.py \
        --input {{ params.tpp_root }}/{{ run_id }}/{{ params.chembl_sdf_path }} \
        --output {{ params.tpp_root }}/{{ run_id }}/{{ params.chembl_sdf_shards_path }} \
        --num-shards {{ params.num_shards }} \
        --num-proc {{ params.num_proc }}
    """
    shard_sdf = BashOperator(
        task_id="shard_sdf",
        bash_command=shard_sdf_command,
        params={"num_shards": 100, "num_proc": 6},
    )

    # SDF to Parquet
    sdf_to_parquet_app = "{{ params.tpp_python_home }}/jobs/sdf_to_parquet.py"
    sdf_to_parquet_app_args = [
        "--input",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.chembl_sdf_shards_path }}",
        "--output",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.output }}",
        "--dataset",
        "{{ params.dataset }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    sdf_to_parquet = SparkSubmitOperator(
        task_id="sdf_to_parquet",
        application=sdf_to_parquet_app,
        application_args=sdf_to_parquet_app_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "output_path": "chembl/compounds.parquet",
            "dataset": "ChEMBL",
            "num_partitions": 200,
        },
    )

    # Process Assays
    process_assays_app = "{{ params.tpp_python_home }}/jobs/chembl/process_assays.py"
    process_assays_app_args = [
        "--input",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.input_path }}",
        "--output",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.output_path }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    process_assays = SparkSubmitOperator(
        task_id="process_assays",
        application=process_assays_app,
        application_args=process_assays_app_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "input_path": "chembl/assays.parquet",
            "output_path": "chembl/assays_processed.parquet",
            "num_partitions": 200,
        },
    )

    # Merge Assays
    merge_assays_app = "{{ params.tpp_python_home }}/jobs/merge_assays.py"
    merge_assays_app_args = [
        "--merge-chembl",
        "--chembl-compounds",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.chembl_compounds_path }}",
        "--chembl-assays",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.chembl_assays_path}}",
        "--output-dir",
        "{{ params.tpp_root }}/{{ run_id }}",
    ]
    merge_assays = SparkSubmitOperator(
        task_id="merge_assays",
        application=merge_assays_app,
        application_args=merge_assays_app_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "chembl_compounds_path": "chembl/compounds.parquet",
            "chembl_assays_path": "chembl/assays_processed.parquet",
        },
    )

    # Compute Semisparse Features - Scala
    compute_semisparse_features_scala_app = "{{ params.tpp_scala_jar }}"
    compute_semisparse_features_scala_args = [
        "-i",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.input_path }}",
        "-o",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.output_path}}",
        "--features",
        "{{ params.feature_type }}",
        "--npartitions",
        "{{ params.num_partitions }}",
    ]
    compute_semisparse_features_scala = SparkSubmitOperator(
        task_id="compute_semisparse_features_scala",
        application=compute_semisparse_features_scala_app,
        application_args=compute_semisparse_features_scala_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "input_path": "merged_data.parquet",
            "output_path": "features_semisparse_partial.parquet",
            "feature_type": "semisparse",
        },
    )

    # Compute Semisparse Features - Python
    compute_semisparse_features_python_app = (
        "{{ params.tpp_python_home }}/jobs/compute_features.py"
    )
    compute_semisparse_features_python_args = [
        "--input",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.input_path }}",
        "--output",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.ouput_path }}",
        "--feature-type",
        "{{ params.feature_type }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    compute_semisparse_features_python = SparkSubmitOperator(
        task_id="compute_semisparse_features_python",
        application=compute_semisparse_features_python_app,
        application_args=compute_semisparse_features_python_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "input_path": "features_semisparse_partial.parquet",
            "output_path": "features_semisparse.parquet",
            "feature_type": "semisparse",
        },
    )

    # Clean Semisparse Features
    clean_semisparse_features_app = (
        "{{ params.tpp_python_home }}/jobs/clean_features.py"
    )
    clean_semisparse_features_args = [
        "--input",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.input_path }}",
        "--output-dir",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.output_dir_path }}",
        "--temp-files",
        "{{ params.tpp_root }}/{{ run_id }}/tmp",
        "--feature",
        "CATS2D",
        "--feature",
        "SHED",
    ]
    clean_semisparse_features = SparkSubmitOperator(
        task_id="clean_semisparse_features",
        application=clean_semisparse_features_app,
        application_args=clean_semisparse_features_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "input_path": "features_semisparse.parquet",
            "output_dir_path": "features_semisparse_clean",
        },
    )

    # Compute Sparse Features - Scala
    compute_sparse_features_scala_app = "{{ params.tpp_scala_jar }}"
    compute_sparse_features_scala_args = [
        "-i",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.input_path }}",
        "-o",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.ouput_path }}",
        "--features",
        "{{ params.feature_type }}",
        "--npartitions",
        "{{ params.num_partitions }}",
    ]
    compute_sparse_features_scala = SparkSubmitOperator(
        task_id="compute_sparse_features_scala",
        application=compute_sparse_features_scala_app,
        application_args=compute_sparse_features_scala_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "input_path": "merged_data.parquet",
            "output_path": "features_sparse.parquet",
            "feature_type": "sparse",
        },
    )

    # Clean Sparse Features
    clean_sparse_features_app = "{{ params.tpp_python_home }}/jobs/clean_features.py"
    clean_sparse_features_args = [
        "--input",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.input_path }}",
        "--output-dir",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.output_dir_path }}",
        "--temp-files",
        "{{ params.tpp_root }}/{{ run_id }}/tmp",
        "--feature",
        "DFS8",
        "--feature",
        "ECFC4",
        "--feature",
        "ECFC6",
    ]
    clean_sparse_features = SparkSubmitOperator(
        task_id="clean_sparse_features",
        application=clean_sparse_features_app,
        application_args=clean_sparse_features_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "input_path": "features_sparse.parquet",
            "output_dir_path": "features_sparse_clean",
        },
    )

    # Compute Tox Features - Python
    compute_tox_features_python_app = (
        "{{ params.tpp_python_home }}/jobs/compute_descriptors.py"
    )
    compute_tox_features_python_args = [
        "--input",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.input_path }}",
        "--output",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.ouput_path }}",
        "--feature-type",
        "{{ params.feature_type }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    compute_tox_features_python = SparkSubmitOperator(
        task_id="compute_tox_features_python",
        application=compute_tox_features_python_app,
        application_args=compute_tox_features_python_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "input_path": "merged_data.parquet",
            "output_path": "features_tox_morgan_partial.parquet",
            "feature_type": "tox",
        },
    )

    # Compute Morgan Fingerprints - Python
    compute_morgan_features_python_app = (
        "{{ params.tpp_python_home }}/jobs/compute_descriptors.py"
    )
    compute_morgan_features_python_args = [
        "--input",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.input_path }}",
        "--output",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.ouput_path }}",
        "--feature-type",
        "{{ params.feature_type }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    compute_morgan_features_python = SparkSubmitOperator(
        task_id="compute_morgan_features_python",
        application=compute_morgan_features_python_app,
        application_args=compute_morgan_features_python_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "input_path": "features_tox_morgan_partial.parquet",
            "output_path": "features_tox_morgan.parquet",
            "feature_type": "morgan",
        },
    )

    export_tfrecords_semisparse_app = (
        "{{ params.tpp_python_home }}/jobs/export_tfrecords.py"
    )
    export_tfrecords_semisparse_app_args = [
        "--input",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.input_path }}",
        "--output-dir",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.output_dir_path }}",
        "--cluster-mapping",
        "{{ params.cluster_mapping_path }}",
        "--feature",
        "CATS2D_clean",
        "--feature",
        "SHED_clean",
        "--feature",
        "PubChem",
        "--feature",
        "maccs_fp",
        "--feature",
        "rdkit_fp",
    ]
    export_tfrecords_semisparse = SparkSubmitOperator(
        task_id="export_tfrecords_semisparse",
        application=export_tfrecords_semisparse_app,
        jars="{{ params.tf_spark_connector }}",
        application_args=export_tfrecords_semisparse_app_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "input_path": "features_semisparse_clean/data_clean.parquet",
            "output_dir_path": "records_semisparse",
        },
    )

    export_tfrecords_sparse_app = (
        "{{ params.tpp_python_home }}/jobs/export_tfrecords.py"
    )
    export_tfrecords_sparse_app_args = [
        "--input",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.input_path }}",
        "--output-dir",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.output_dir_path }}",
        "--cluster-mapping",
        "{{ params.cluster_mapping_path }}",
        "--feature",
        "CATS2D_clean",
        "--feature",
        "SHED_clean",
        "--feature",
        "PubChem",
        "--feature",
        "maccs_fp",
        "--feature",
        "rdkit_fp",
    ]
    export_tfrecords_sparse = SparkSubmitOperator(
        task_id="export_tfrecords_sparse",
        application=export_tfrecords_sparse_app,
        application_args=export_tfrecords_sparse_app_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "input_path": "features_sparse_clean/data_clean.parquet",
            "output_dir_path": "records_sparse",
        },
    )

    export_tfrecords_tox_morgan_app = (
        "{{ params.tpp_python_home }}/jobs/export_tfrecords.py"
    )
    export_tfrecords_tox_morgan_app_args = [
        "--input",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.input_path }}",
        "--output-dir",
        "{{ params.tpp_root }}/{{ run_id }}/{{ params.output_dir_path }}",
        "--cluster-mapping",
        "{{ params.cluster_mapping_path }}",
        "--feature",
        "morgan_fp",
        "--feature",
        "tox_fp",
    ]
    export_tfrecords_tox_morgan = SparkSubmitOperator(
        task_id="export_tfrecords_tox_morgan",
        application=export_tfrecords_tox_morgan_app,
        application_args=export_tfrecords_tox_morgan_app_args,
        conf={"spark.pyspark.python": "{{ params.conda_prefix }}/bin/python"},
        params={
            "input_path": "features_tox_morgan_clean/data_clean.parquet",
            "output_dir_path": "records_tox_morgan",
        },
    )

# Preprocessing + Protocol
create_folder_structure >> download_chembl >> extract_chembl
extract_chembl >> export_sqlite_assays >> process_assays
extract_chembl >> shard_sdf >> sdf_to_parquet
[process_assays, sdf_to_parquet] >> merge_assays

# Compute Semisparse Features
merge_assays >> compute_semisparse_features_scala >> compute_semisparse_features_python
compute_semisparse_features_python >> clean_semisparse_features

# Compute Sparse Features
merge_assays >> compute_sparse_features_scala >> clean_sparse_features

# Compute Tox + Morgan Features
merge_assays >> compute_tox_features_python >> compute_morgan_features_python

clean_semisparse_features >> export_tfrecords_semisparse
clean_sparse_features >> export_tfrecords_sparse
compute_morgan_features_python >> export_tfrecords_tox_morgan


if __name__ == "__main__":
    dag.cli()
