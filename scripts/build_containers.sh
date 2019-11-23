#!/bin/bash

AIRFLOW_HOME=/home/airflow/airflow

SECRETS_DIR=/run/secrets
TPP_CREDENTIALS="./worker-secrets"
PYSPARK_PYTHON="/system/user/ruch/miniconda3/envs/airflow-worker"

docker build \
    --build-arg AIRFLOW_HOME=${AIRFLOW_HOME} \
    -f tools/docker/airflow-core.Dockerfile \
    -t ml-jku/airflow-core \
    .

docker build \
    -f tools/docker/airflow.Dockerfile \
    -t ml-jku/airflow \
    .

docker build \
    --build-arg SECRETS_DIR=${SECRETS_DIR} \
    --build-arg TPP_CREDENTIALS=${TPP_CREDENTIALS} \
    --build-arg PYSPARK_PYTHON=${PYSPARK_PYTHON} \
    -f tools/docker/airflow-worker.Dockerfile \
    -t ml-jku/airflow-worker \
    .