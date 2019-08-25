#!/bin/bash

DB_SERVER="igne"
DB_SERVER_DOCKER_IP="172.18.0.1"
AIRFLOW_NETWORK="tpp_airflow_tpp"

docker run --rm \
    --env-file airflow.env \
    --network "${AIRFLOW_NETWORK}" \
    --add-host "${DB_SERVER}:${DB_SERVER_DOCKER_IP}" \
    -ti ml-jku/airflow \
    airflow initdb 