#!/bin/bash

DB_HOST="demosite.ml.jku.at"
DB_HOST_DOCKER_IP="172.18.0.1"
AIRFLOW_ENV=".docker.env"
AIRFLOW_NETWORK="tpp"
SECRETS_DIR="/run/secrets"
TPP_HOME="/publicdata/tpp"

docker run --rm \
    --env-file "${AIRFLOW_ENV}" \
    --network "${AIRFLOW_NETWORK}" \
    --add-host "${DB_HOST}:${DB_HOST_DOCKER_IP}" \
    -v "$(pwd)"/secrets/ca-cert.pem:${SECRETS_DIR}/ca-cert \
    -v "$(pwd)"/secrets/client-key.pem:"${SECRETS_DIR}"/postgres-client-key \
    -v "$(pwd)"/secrets/client-cert.pem:"${SECRETS_DIR}"/postgres-client-cert \
    -v "$(pwd)"/secrets/client-key.pem:"${SECRETS_DIR}"/rabbitmq-client-key \
    -v "$(pwd)"/secrets/client-cert.pem:"${SECRETS_DIR}"/rabbitmq-client-cert \
    -ti ml-jku/airflow \
    airflow connections --add --conn_id spark_raptor --conn_type spark --conn_host spark://airflow-spark:7077 --conn_extra '{"queue": "root.default"}'

docker run --rm \
    --env-file "${AIRFLOW_ENV}" \
    --network "${AIRFLOW_NETWORK}" \
    --add-host "${DB_HOST}:${DB_HOST_DOCKER_IP}" \
    -v "$(pwd)"/secrets/ca-cert.pem:${SECRETS_DIR}/ca-cert \
    -v "$(pwd)"/secrets/client-key.pem:"${SECRETS_DIR}"/postgres-client-key \
    -v "$(pwd)"/secrets/client-cert.pem:"${SECRETS_DIR}"/postgres-client-cert \
    -v "$(pwd)"/secrets/client-key.pem:"${SECRETS_DIR}"/rabbitmq-client-key \
    -v "$(pwd)"/secrets/client-cert.pem:"${SECRETS_DIR}"/rabbitmq-client-cert \
    -v ${TPP_HOME}/code/tpp_airflow/configs:${AIRFLOW_HOME}/configs \
    -ti ml-jku/airflow \
    airflow variables --import ${AIRFLOW_HOME}/configs/dag_variables.json
