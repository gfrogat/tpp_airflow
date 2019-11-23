#!/bin/env bash

AIRFLOW_ENV=".docker.env"
AIRFLOW_HOME="/home/airflow/airflow"
TPP_HOME="/publicdata/tpp"
SPARK_HOME="/opt/spark-2.4.3-bin-hadoop2.7"

docker run --rm \
    --env-file "${AIRFLOW_ENV}" \
    -v /local10:/local10 \
    -v /local12:/local12 \
    -v ${TPP_HOME}:${TPP_HOME} \
    -v ${TPP_HOME}/code/tpp_airflow/dags:${AIRFLOW_HOME}/dags \
    -v ${TPP_HOME}/code/tpp_airflow/configs:${AIRFLOW_HOME}/configs \
    -v ${TPP_HOME}/logs:${AIRFLOW_HOME}/logs \
    -v "$(pwd)/tools/spark/conf-$(hostname)":"${SPARK_HOME}/conf" \
    -p 7077:7077 -p 8080:8080 -p 4040:4040 \
    -ti ml-jku/airflow-worker \
    /bin/bash

echo "Worker started"
