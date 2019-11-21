#!/bin/env bash

AIRFLOW_ENV=".docker.env"
AIRFLOW_HOME="/home/airflow/airflow"

docker run --rm \
    --env-file "${AIRFLOW_ENV}" \
    -v /local10:/local10 \
    -v /publicdata/tpp:/publicdata/tpp \
    -v $(pwd)/dags:${AIRFLOW_HOME}/dags \
    -v $(pwd)/configs:${AIRFLOW_HOME}/configs \
    -ti ml-jku/airflow-worker \
    /bin/bash #airflow worker

echo "Worker started"
