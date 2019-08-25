#!/bin/bash

docker build \
    -f tools/docker/airflow.Dockerfile \
    -t ml-jku/airflow \
    .