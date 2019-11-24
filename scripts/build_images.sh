#!/bin/bash

AIRFLOW_HOME=/home/airflow/airflow

SECRETS_DIR=/run/secrets
TPP_CREDENTIALS="./worker-secrets"
RELEASE="v0.1.0"

docker build \
    --build-arg AIRFLOW_HOME=${AIRFLOW_HOME} \
    -f tools/docker/airflow-core.Dockerfile \
    -t ml-jku/airflow-core:${RELEASE} \
    .

docker build \
    -f tools/docker/airflow.Dockerfile \
    -t ml-jku/airflow:${RELEASE} \
    .

docker build \
    --build-arg SECRETS_DIR=${SECRETS_DIR} \
    --build-arg TPP_CREDENTIALS=${TPP_CREDENTIALS} \
    -f tools/docker/airflow-worker.Dockerfile \
    -t ml-jku/airflow-worker:${RELEASE} \
    .

# Tag latest containers
docker tag ml-jku/airflow:${RELEASE} ml-jku/airflow:latest
docker tag ml-jku/airflow-worker:${RELEASE} ml-jku/airflow-worker:latest

# As we don't have a internal docker registry simply store containers on NFS
docker save ml-jku/airflow:${RELEASE} | gzip > /publicdata/tpp/docker/airflow_${RELEASE}.tar.gz
docker save ml-jku/airflow-worker:${RELEASE} | gzip > /publicdata/tpp/docker/airflow-worker_${RELEASE}.tar.gz

docker save ml-jku/airflow:latest | gzip > /publicdata/tpp/docker/airflow_latest.tar.gz
docker save ml-jku/airflow-worker:latest | gzip > /publicdata/tpp/docker/airflow-worker_latest.tar.gz