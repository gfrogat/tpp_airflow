
FROM python:3.7-slim-buster
LABEL maintainer="Peter Ruch"

ENV DEBIAN_FRONTEND noninteractive

ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

COPY script/entrypoint.sh /entrypoint.sh
COPY requirements.txt /requirements.txt

RUN pip3 install -f requirements.txt

EXPOSE 8080

USER airflow
ENTRYPOINT ["/airflow_entrypoint.sh"]