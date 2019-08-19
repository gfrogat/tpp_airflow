from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
import datetime as dt


default_args = {
    "owner": "tpp",
    "depends_on_past": False,
    "start_date": dt.datetime(2019, 8, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=30),
}

with DAG("pipeline_chembl", default_args=default_args, schedule_interval=None) as dag:

    download_chembl_command = """
    CHEMBL_DIR={{ params.chembl_dir}} \
    RELEASE={{ params.chembl_release}} \
    bash {{ params.tpp_home }}/scripts/download_chembl.sh
    """
    download_chembl = BashOperator(
        task_id="download_chembl",
        bash_command=download_chembl_command,
        params={
            "tpp_home": "/Users/gfrogat/Projects/github.com/gfrogat/tpp",
            "chembl_dir": "/Users/gfrogat/Downloads/data",
            "chembl_release": 25,
        },
    )

    export_sqlite_command = """
    python {{ params.tpp_home }}/scripts/export_chembl_sqlite.py \
        --input {{ params.input_path }} \
        --output {{ params.output_path }} \
        --export {{ params.export_type }}
    """
    export_sqlite_assays = BashOperator(
        task_id="export_sqlite_assays",
        bash_command=export_sqlite_command,
        params={
            "tpp_home": "/Users/gfrogat/Projects/github.com/gfrogat/tpp",
            "input_path": "",
            "output_path": "",
            "export_type": "assays",
        },
    )

    export_sqlite_compounds = BashOperator(
        task_id="export_sqlite_compounds",
        bash_command=export_sqlite_command,
        params={
            "tpp_home": "/Users/gfrogat/Projects/github.com/gfrogat/tpp",
            "input_path": "",
            "output_path": "",
            "export_type": "compounds",
        },
    )

    shard_sdf_command = """
    python {{ params.tpp_home }}/scripts/shard_sdf.py \
        --input {{ params.input_path }} \
        --output {{ params.output_path }} \
        --num-shards {{ params.num_shards }} \
        --num-proc {{ params.num_proc }}
    """
    shard_sdf = BashOperator(
        task_id="shard_sdf",
        bash_command=shard_sdf_command,
        params={
            "tpp_home": "/Users/gfrogat/Projects/github.com/gfrogat/tpp",
            "input_path": "",
            "output_path": "",
            "num_shards": 100,
            "num_proc": 6,
        },
    )

    sdf_to_parquet_app = """
    {{ params.tpp_home }}/jobs/sdf_to_parquet.py
    """
    sdf_to_parquet_app_args = """
        --input {{ params.input }} \
        --output {{ params.output }} \
        --dataset {{ params.dataset }} \
        --num-partitions {{ params.num_partitions }}
    """
    sdf_to_parquet = SparkSubmitOperator(
        task_id="sdf_to_parquet",
        application=sdf_to_parquet_app,
        application_args=sdf_to_parquet_app_args,
        params={"input": "", "output": "", "dataset": "ChEMBL", "num_partitions": 200},
    )

    merge_assays_app = """
    {{ params.tpp_home }}/jobs/sdf_to_parquet.py
    """
    merge_assays_app_args = """
    """
    merge_assays = SparkSubmitOperator(
        task_id="merge_assays",
        application=merge_assays_app,
        application_args=merge_assays_app_args,
        params={},
    )

download_chembl >> [export_sqlite_compounds, export_sqlite_assays]
download_chembl >> shard_sdf >> sdf_to_parquet
[export_sqlite_assays, export_sqlite_compounds, sdf_to_parquet] >> merge_assays
