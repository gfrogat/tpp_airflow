import datetime as dt

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
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

dag_config = Variable.get("all_config.json", deserialize_json=True)

with DAG(
    "pipeline_all", default_args=default_args, schedule_interval=None, params=dag_config
) as dag:

    # Create Folder Structure
    create_folder_structure_command = """
    mkdir -p {{ params.tpp_root }} && \
    mkdir -p {{ params.chembl_root }}
    """
    create_folder_structure = BashOperator(
        task_id="create_folder_structure", bash_command=create_folder_structure_command
    )

    # Download ChEMBL data
    download_chembl_command = """
    CHEMBL_DIR={{ params.chembl_root }}/data \
    RELEASE={{ params.chembl_release }} \
    bash {{ params.tpp_python_home }}/scripts/download_chembl.sh
    """
    download_chembl = BashOperator(
        task_id="download_chembl", bash_command=download_chembl_command
    )

    # Extract downloaded ChEMBL data
    extract_chembl_command = """
    pushd {{ params.chembl_root }}/data/chembl_{{ params.chembl_release }} && \
    gunzip chembl_*.sdf.gz && \
    tar --strip-components=1 -zxf chembl_*_sqlite.tar.gz && \
    popd
    """
    extract_chembl = BashOperator(
        task_id="extract_chembl", bash_command=extract_chembl_command
    )

    # Export ChEMBL SQLite Assays
    export_sqlite_command = """
    python {{ params.tpp_python_home }}/scripts/export_chembl_sqlite.py \
        --input {{ params.chembl_root }}/{{ params.chembl_sqlite_path}} \
        --output {{ params.chembl_root }}/{{ params.output_path }} \
        --export {{ params.export_type }}
    """
    export_sqlite_assays = BashOperator(
        task_id="export_sqlite_assays",
        bash_command=export_sqlite_command,
        params={"output_path": "assays.parquet", "export_type": "assays"},
    )

    # Shard ChEMBL SDF
    shard_sdf_command = """
    python {{ params.tpp_python_home }}/scripts/shard_sdf.py \
        --input {{ params.chembl_root }}/{{ params.chembl_sdf_path }} \
        --output {{ params.chembl_root}}/{{ params.chembl_sdf_shards_path }} \
        --num-shards {{ params.num_shards }} \
        --num-proc {{ params.num_proc }}
    """
    shard_sdf = BashOperator(
        task_id="shard_sdf",
        bash_command=shard_sdf_command,
        params={"num_shards": 100, "num_proc": 6},
    )

    # ChEMBL SDF to Parquet
    chembl_sdf_to_parquet_app = "{{ params.tpp_python_home }}/jobs/sdf_to_parquet.py"
    chembl_sdf_to_parquet_app_args = [
        "--input",
        "{{ params.chembl_root }}/{{ params.chembl_sdf_shards_path }}",
        "--output",
        "{{ params.chembl_root }}/{{ params.output }}",
        "--dataset",
        "{{ params.dataset }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    chembl_sdf_to_parquet = SparkSubmitOperator(
        task_id="chembl_sdf_to_parquet",
        application=chembl_sdf_to_parquet_app,
        application_args=chembl_sdf_to_parquet_app_args,
        params={
            "output_path": "compounds.parquet",
            "dataset": "ChEMBL",
            "num_partitions": 200,
        },
    )

    # Process ChEMBL Assays
    process_chembl_assays_app = (
        "{{ params.tpp_python_home }}/jobs/chembl/process_assays.py"
    )
    process_chembl_assays_app_args = [
        "--input",
        "{{ params.input_path }}",
        "--output",
        "{{ params.output_path }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    process_chembl_assays = SparkSubmitOperator(
        task_id="process_chembl_assays",
        application=process_chembl_assays_app,
        application_args=process_chembl_assays_app_args,
        params={
            "input_path": "assays.parquet",
            "output_path": "assays_processed.parquet",
            "num_partitions": 200,
        },
    )

    # Download PubChem data
    download_pubchem_command = """
    PUBCHEM_DIR={{ params.pubchem_root }}/data \
    bash {{ params.tpp_python_home }}/scripts/download_pubchem.sh
    """
    download_pubchem = BashOperator(
        task_id="download_pubchem", bash_command=download_pubchem_command
    )

    # Export PubChem Assays
    export_pubchem_assays_app = """

    """
    export_pubchem_assays_app_args = """

    """
    export_pubchem_assays = SparkSubmitOperator(
        task_id="export_pubchem_assays",
        application=export_pubchem_assays_app,
        application_args=export_pubchem_assays_app_args,
        params={},
    )

    # Process PubChem Assays
    process_pubchem_assays_app = """

    """
    process_pubchem_assays_app_args = """

    """
    process_pubchem_assays = SparkSubmitOperator(
        task_id="process_pubchem_assays",
        application=process_pubchem_assays_app,
        application_args=process_pubchem_assays_app_args,
        params={},
    )

    # PubChem SDF to Parquet
    pubchem_sdf_to_parquet_app = "{{ params.tpp_python_home }}/jobs/sdf_to_parquet.py"
    pubchem_sdf_to_parquet_app_args = [
        "--input",
        "{{ params.chembl_root }}/{{ params.chembl_sdf_shards_path }}",
        "--output",
        "{{ params.chembl_root }}/{{ params.output }}",
        "--dataset",
        "{{ params.dataset }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    pubchem_sdf_to_parquet = SparkSubmitOperator(
        task_id="pubchem_sdf_to_parquet",
        application=pubchem_sdf_to_parquet_app,
        application_args=pubchem_sdf_to_parquet_app_args,
        params={
            "output_path": "compounds.parquet",
            "dataset": "ChEMBL",
            "num_partitions": 200,
        },
    )

    # Download ZINC15 data
    download_zinc15_command = """
    python {{ params.tpp_python_home }}/scripts/download_zinc15.py
        --output {{ params.output_path }}
    """
    download_zinc15 = BashOperator(
        task_id="download_zinc15",
        bash_command=download_zinc15_command,
        params={"output_path": ""},
    )

    # Process ZINC15 Data
    process_zinc15_data_app = """

    """
    process_zinc15_data_app_args = """

    """
    process_zinc15_data = SparkSubmitOperator(
        task_id="process_zinc15_data",
        application=process_zinc15_data_app,
        application_args=process_zinc15_data_app_args,
        params={},
    )

    # Merge Assays
    merge_assays_app = "{{ params.tpp_python_home }}/jobs/merge_assays.py"
    merge_assays_app_args = [
        "--merge-chembl",
        "--chembl-compounds",
        "{{ params.chembl_root }}/{{ params.chembl_compounds_path }}",
        "--chembl-assays",
        "{{ params.chembl_root }}/{{ params.chembl_assays_path}}",
        "--output-dir",
        "{{ params.tpp_root }}",
    ]
    merge_assays = SparkSubmitOperator(
        task_id="merge_assays",
        application=merge_assays_app,
        application_args=merge_assays_app_args,
        params={
            "chembl_compounds_path": "compounds.parquet",
            "chembl_assays_path": "assays_processed.parquet",
        },
    )

    # Compute Semisparse Features - Scala
    compute_semisparse_features_scala_app = "{{ params.tpp_scala_jar }}"
    compute_semisparse_features_scala_args = [
        "-i",
        "{{ params.tpp_root }}/{{ paramns.input_path }}",
        "-o",
        "{{ params.tpp_root }}/merged_data_semisparse_partial.parquet",
        "--features",
        "{{ params.feature_type }}",
        "--npartitions",
        "{{ params.num_partitions }}",
    ]
    compute_semisparse_features_scala = SparkSubmitOperator(
        task_id="compute_semisparse_features_scala",
        application=compute_semisparse_features_scala_app,
        application_args=compute_semisparse_features_scala_args,
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
        "{{ params.tpp_root }}/{{ params.input_path }}",
        "--output",
        "{{ params.tpp_root }}/{{ params.ouput_path }}",
        "--feature-type",
        "{{ params.feature_type }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    compute_semisparse_features_python = SparkSubmitOperator(
        task_id="compute_semisparse_features_python",
        application=compute_semisparse_features_python_app,
        application_args=compute_semisparse_features_python_args,
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
        "{{ params.tpp_root }}/{{ params.input_path }}",
        "--output-dir",
        "{{ params.tpp_root }}/{{ params.output_dir_path }}",
        "--temp-files",
        "{{ params.tmp_dir }}",
        "--feature",
        "CATS2D",
        "--feature",
        "SHED",
    ]
    clean_semisparse_features = SparkSubmitOperator(
        task_id="clean_semisparse_features",
        application=clean_semisparse_features_app,
        application_args=clean_semisparse_features_args,
        params={
            "input_path": "features_semisparse.parquet",
            "output_dir_path": "features_semisparse_clean",
        },
    )

    # Compute Sparse Features - Scala
    compute_sparse_features_scala_app = "{{ params.tpp_scala_jar }}"
    compute_sparse_features_scala_args = [
        "-i",
        "{{ params.tpp_root }}/{{ params.input_path }}",
        "-o",
        "{{ params.tpp_root }}/{{ params.ouput_path }}",
        "--features",
        "{{ params.feature_type }}",
        "--npartitions",
        "{{ params.num_partitions }}",
    ]
    compute_sparse_features_scala = SparkSubmitOperator(
        task_id="compute_sparse_features_scala",
        application=compute_sparse_features_scala_app,
        application_args=compute_sparse_features_scala_args,
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
        "{{ params.tpp_root }}/{{ params.input_path }}",
        "--output-dir",
        "{{ params.tpp_root }}/{{ params.output_dir_path }}",
        "--temp-files",
        "{{ params.tmp_dir }}",
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
        params={
            "input_path": "features_sparse.parquet",
            "output_dir_path": "features_sparse_clean",
        },
    )

    # Compute Tox Features - Python
    compute_tox_features_python_app = (
        "{{ params.tpp_python_home }}/jobs/compute_features.py"
    )
    compute_tox_features_python_args = [
        "--input",
        "{{ params.tpp_root }}/{{ params.input_path }}",
        "--output",
        "{{ params.tpp_root }}/{{ params.ouput_path }}",
        "--feature-type",
        "{{ params.feature_type }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    compute_tox_features_python = SparkSubmitOperator(
        task_id="compute_tox_features_python",
        application=compute_tox_features_python_app,
        application_args=compute_tox_features_python_args,
        params={
            "input_path": "merged_data.parquet",
            "output_path": "features_tox_morgan_partial.parquet",
            "feature_type": "tox",
        },
    )

    # Compute Morgan Fingerprints - Python
    compute_morgan_features_python_app = (
        "{{ params.tpp_python_home }}/jobs/compute_features.py"
    )
    compute_morgan_features_python_args = [
        "--input",
        "{{ params.tpp_root }}/{{ params.input_path }}",
        "--output",
        "{{ params.tpp_root }}/{{ params.ouput_path }}",
        "--feature-type",
        "{{ params.feature_type }}",
        "--num-partitions",
        "{{ params.num_partitions }}",
    ]
    compute_morgan_features_python = SparkSubmitOperator(
        task_id="compute_morgan_features_python",
        application=compute_morgan_features_python_app,
        application_args=compute_morgan_features_python_args,
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
        "{{ params.tpp_root }}/{{ params.input_path }}",
        "--output-dir",
        "{{ params.tpp_root }}/{{ params.output_dir_path }}",
        "--cluster-mapping",
        "{{ params.tpp_root }}/{{ params.cluster_mapping_path }}",
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
        "{{ params.tpp_root }}/{{ params.input_path }}",
        "--output-dir",
        "{{ params.tpp_root }}/{{ params.output_dir_path }}",
        "--cluster-mapping",
        "{{ params.tpp_root }}/{{ params.cluster_mapping_path }}",
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
        "{{ params.tpp_root }}/{{ params.input_path }}",
        "--output-dir",
        "{{ params.tpp_root }}/{{ params.output_dir_path }}",
        "--cluster-mapping",
        "{{ params.tpp_root }}/{{ params.cluster_mapping_path }}",
        "--feature",
        "morgan_fp",
        "--feature",
        "tox_fp",
    ]
    export_tfrecords_tox_morgan = SparkSubmitOperator(
        task_id="export_tfrecords_tox_morgan",
        application=export_tfrecords_tox_morgan_app,
        application_args=export_tfrecords_tox_morgan_app_args,
        params={
            "input_path": "features_tox_morgan_clean/data_clean.parquet",
            "output_dir_path": "records_tox_morgan",
        },
    )

# Preprocessing + Protocol
create_folder_structure >> [download_chembl, download_pubchem, download_zinc15]

# ChEMBL
download_chembl >> extract_chembl >> export_sqlite_assays >> process_chembl_assays
extract_chembl >> shard_sdf >> chembl_sdf_to_parquet
[process_chembl_assays, chembl_sdf_to_parquet] >> merge_assays

# PubChem
download_pubchem >> export_pubchem_assays >> process_pubchem_assays
download_pubchem >> pubchem_sdf_to_parquet
[process_pubchem_assays, pubchem_sdf_to_parquet] >> merge_assays

# ZINC15
download_zinc15 >> process_zinc15_data >> merge_assays

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
