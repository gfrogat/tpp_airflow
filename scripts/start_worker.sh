#!/bin/env bash

AIRFLOW_ENV=".docker.env"
AIRFLOW_HOME="/home/airflow/airflow"
TPP_HOME="/publicdata/tpp"
PYSPARK_PYTHON="/system/user/ruch/miniconda3/envs/airflow-worker"

docker run --rm \
    --env-file "${AIRFLOW_ENV}" \
    -v /local10:/local10 \
    -v ${TPP_HOME}:${TPP_HOME} \
    -v ${TPP_HOME}/code/tpp_airflow/dags:${AIRFLOW_HOME}/dags \
    -v ${TPP_HOME}/code/tpp_airflow/configs:${AIRFLOW_HOME}/configs \
    -v ${TPP_HOME}/logs:${AIRFLOW_HOME}/logs \
    -v ${PYSPARK_PYTHON}:${PYSPARK_PYTHON} \
    -ti ml-jku/airflow-worker \
    /bin/bash #airflow worker

echo "Worker started"
