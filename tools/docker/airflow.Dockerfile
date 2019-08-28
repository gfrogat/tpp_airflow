
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
    && rm -rf /var/lib/apt/lists/*

RUN echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen
RUN locale-gen en_US.UTF-8

ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

COPY tools/docker/scripts/airflow_entrypoint.sh /entrypoint.sh
COPY requirements.txt /requirements.txt

RUN pip3 install --no-cache -U pip setuptools wheel \
    && pip3 install --no-cache -r /requirements.txt

EXPOSE 8080 5555

RUN useradd --create-home --shell /bin/bash --no-log-init airflow

ENV AIRFLOW_HOME ${AIRFLOW_HOME}
RUN mkdir ${AIRFLOW_HOME} && chown airflow:airflow ${AIRFLOW_HOME}

USER airflow
WORKDIR /home/airflow


ENTRYPOINT ["/entrypoint.sh"]
