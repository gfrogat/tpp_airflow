FROM python:3.7-slim-buster
LABEL maintainer="Peter Ruch"

ARG AIRFLOW_HOME=/home/airflow/airflow

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -y && apt-get install -y \
    --no-install-recommends \
    apt-utils \
    build-essential \
    curl \
    git \
    locales \
    rsync \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen
RUN locale-gen en_US.UTF-8

ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

RUN useradd --create-home --shell /bin/bash --no-log-init airflow

ENV AIRFLOW_HOME ${AIRFLOW_HOME}
RUN mkdir ${AIRFLOW_HOME} && chown airflow:airflow ${AIRFLOW_HOME}

COPY tools/docker/scripts/launch_airflow.sh /launch_airflow.sh

USER airflow
WORKDIR /home/airflow

ENTRYPOINT ["/launch_airflow.sh"]