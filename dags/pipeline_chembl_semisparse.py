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

dag_config = Variable.get("chembl_semisparse_config.json", deserialize_json=True)

with DAG(
    "pipeline_chembl_semisparse",
    default_args=default_args,
    schedule_interval=None,
    params=dag_config,
) as dag:

    create_folder_structure_command = """
    mkdir -p {{ params.tpp_root }} && \
    mkdir -p {{ params.chembl_root }}
    """
    create_folder_structure = BashOperator(
        task_id="create_folder_structure", bash_command=create_folder_structure_command
    )

    download_chembl_command = """
    CHEMBL_DIR={{ params.chembl_root }}/data \
    RELEASE={{ params.chembl_release }} \
    bash {{ params.tpp_python_home }}/scripts/download_chembl.sh
    """
    download_chembl = BashOperator(
        task_id="download_chembl", bash_command=download_chembl_command
    )

    extract_chembl_command = """
    pushd {{ params.chembl_root }}/data/chembl_{{ params.chembl_release }} && \
    gunzip chembl_*.sdf.gz && \
    tar --strip-components=1 -zxf chembl_*_sqlite.tar.gz && \
    popd
    """
    extract_chembl = BashOperator(
        task_id="extract_chembl", bash_command=extract_chembl_command
    )

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

    sdf_to_parquet_app = """
    {{ params.tpp_python_home }}/jobs/sdf_to_parquet.py
    """
    sdf_to_parquet_app_args = """
        --input {{ params.chembl_root }}/{{ params.chembl_sdf_shards_path }} \
        --output {{ params.chembl_root }}/{{ params.output }} \
        --dataset {{ params.dataset }} \
        --num-partitions {{ params.num_partitions }}
    """
    sdf_to_parquet = SparkSubmitOperator(
        task_id="sdf_to_parquet",
        application=sdf_to_parquet_app,
        application_args=sdf_to_parquet_app_args,
        params={
            "output_path": "compounds.parquet",
            "dataset": "ChEMBL",
            "num_partitions": 200,
        },
    )

    process_assays_app = """
    {{ params.tpp_python_home }}/jobs/chembl/process_assays.py
    """
    process_assays_app_args = """
        --input {{ params.input_path }} \
        --output {{ params.output_path }} \
        --num-partitions {{ params.num_partitions }}
    """
    process_assays = SparkSubmitOperator(
        task_id="process_assays",
        application=process_assays_app,
        application_args=process_assays_app_args,
        params={
            "input_path": "assays.parquet",
            "output_path": "assays_processed.parquet",
            "num_partitions": 200,
        },
    )

    merge_assays_app = """
    {{ params.tpp_python_home }}/merge_assays.py
    """
    merge_assays_app_args = """
        --merge-chembl \
        --chembl-compounds {{ params.chembl_root }}/{{ params.chembl_compounds_path }} \
        --chembl-assays {{ params.chembl_root }}/{{ params.chembl_assays_path}} \
        --output-dir {{ params.tpp_root }}
    """
    merge_assays = SparkSubmitOperator(
        task_id="merge_assays",
        application=merge_assays_app,
        application_args=merge_assays_app_args,
        params={
            "chembl_compounds_path": "compounds.parquet",
            "chembl_assays_path": "assays_processed.parquet",
        },
    )

    compute_descriptors_scala_app = """
    {{ params.tpp_scala_jar }}
    """
    compute_descriptors_scala_args = """
        -i {{ params.tpp_root }}/{{ paramns.input_path }} \
        -o {{ params.tpp_root }}/merged_data_semisparse_partial.parquet \
        --features {{ params.feature_type }} \
        --npartitions {{ params.num_partitions }}
    """
    compute_descriptors_scala = SparkSubmitOperator(
        task_id="compute_descriptors_scala",
        application=compute_descriptors_scala_app,
        application_args=compute_descriptors_scala_args,
        params={
            "input_path": "merged_data.parquet",
            "output_path": "features_semisparse_partial.parquet",
        },
    )

    compute_descriptors_python_app = """
    {{ params.tpp_python_home }}/compute_descriptors.py
    """
    compute_descriptors_python_args = """
        --input {{ params.tpp_root }}/{{ paramns.input_path }} \
        --output {{ params.tpp_root }}/merged_data_semisparse_partial.parquet \
        --feature-type {{ params.feature_type }} \
        --num-partitions {{ params.num_partitions }}
    """
    compute_descriptors_python = SparkSubmitOperator(
        task_id="compute_descriptors_python",
        application=compute_descriptors_python_app,
        application_args=compute_descriptors_python_args,
        params={
            "input_path": "features_semisparse_partial.parquet",
            "output_path": "features_semisparse.parquet",
        },
    )

    clean_features_app = """
    {{ params.tpp_python_home }}/clean_features.py
    """
    clean_features_args = """
        --input {{ params.tpp_root }}/{{ params.input_path }} \
        --output-dir {{ params.tpp_root }}/{{ params:output_dir_path }} \
        --temp-files {{ params.tmp_dir }} \
        --feature CATS2D --feature SHED
    """
    clean_features = SparkSubmitOperator(
        task_id="clean_features",
        application=clean_features_app,
        application_args=clean_features_args,
        params={
            "input_path": "features_semisparse.parquet",
            "output_dir_path": "features_clean",
        },
    )


create_folder_structure >> download_chembl >> extract_chembl
extract_chembl >> export_sqlite_assays >> process_assays
extract_chembl >> shard_sdf >> sdf_to_parquet
[process_assays, sdf_to_parquet] >> merge_assays

merge_assays >> compute_descriptors_scala >> compute_descriptors_python
compute_descriptors_python >> clean_features

if __name__ == "__main__":
    dag.cli()
