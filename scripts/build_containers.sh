#!/bin/bash

docker build \
    -f tools/docker/airflow.Dockerfile \
    -t ml-jku/airflow \
    .

docker build \
    --build-arg TPP_CREDENTIALS=./worker-secrets \
    -f tools/docker/worker.Dockerfile \
    -t ml-jku/airflow-worker \
    .