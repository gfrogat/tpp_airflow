#!/bin/bash

DB_SERVER="igne"
DB_SERVER_DOCKER_IP="172.18.0.1"
AIRFLOW_ENV="airflow.env"
AIRFLOW_NETWORK="tpp_airflow_tpp"
SECRETS_DIR="/run/secrets"

docker run --rm \
    --env-file "${AIRFLOW_ENV}" \
    --network "${AIRFLOW_NETWORK}" \
    --add-host "${DB_SERVER}:${DB_SERVER_DOCKER_IP}" \
    -v "$(pwd)"/secrets/ca-cert.pem:${SECRETS_DIR}/ca-cert \
    -v "$(pwd)"/secrets/postgres/client-key.pem:"${SECRETS_DIR}"/postgres-client-key \
    -v "$(pwd)"/secrets/postgres/client-cert.pem:"${SECRETS_DIR}"/postgres-client-cert \
    -v "$(pwd)"/secrets/rabbitmq/client-key.pem:"${SECRETS_DIR}"/rabbitmq-client-key \
    -v "$(pwd)"/secrets/rabbitmq/client-cert.pem:"${SECRETS_DIR}"/rabbitmq-client-cert \
    -ti ml-jku/airflow \
    airflow resetdb