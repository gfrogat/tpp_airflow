
FROM python:3.7-slim-buster
LABEL maintainer="Peter Ruch"

ENV DEBIAN_FRONTEND noninteractive

ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

RUN apt-get update -y && apt-get install -y \
    --no-install-recommends \
    apt-utils \
    build-essential \
    curl \
    git \
    locales \
    rsync \
    && rm -rf /var/lib/apt/lists/*


COPY tools/docker/scripts/airflow_entrypoint.sh /entrypoint.sh
COPY requirements.txt /requirements.txt

RUN pip3 install -U pip setuptools wheel \
    && pip3 install -r /requirements.txt

EXPOSE 8080 5555

RUN echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen
RUN locale-gen en_US.UTF-8

RUN useradd --create-home --shell /bin/bash --no-log-init airflow 
USER airflow

ENV AIRFLOW_HOME /home/airflow
WORKDIR /home/airflow

ENTRYPOINT ["/entrypoint.sh"]
