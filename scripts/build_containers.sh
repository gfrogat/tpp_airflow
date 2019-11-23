#!/bin/bash

AIRFLOW_HOME=/home/airflow/airflow

SECRETS_DIR=/run/secrets
TPP_CREDENTIALS="./worker-secrets"

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
    -f tools/docker/airflow-worker.Dockerfile \
    -t ml-jku/airflow-worker \
    .


docker save ml-jku/airflow:latest | gzip > /publicdata/tpp/docker/airflow_latest.tar.gz
docker save ml-jku/airflow-worker:latest | gzip > /publicdata/tpp/docker/airflow-worker_latest.tar.gz
