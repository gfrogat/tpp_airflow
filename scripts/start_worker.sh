#!/bin/env bash

AIRFLOW_ENV=".docker.env"

docker run --rm \
    --env-file "${AIRFLOW_ENV}" \
    -v /local10:/local00 \
    -v /publicdata/tpp:/publicdata/tpp \
    -ti ml-jku/airflow-worker \
    airflow worker

echo "Worker started"