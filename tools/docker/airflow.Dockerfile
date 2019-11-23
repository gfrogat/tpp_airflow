

FROM ml-jku/airflow-core
LABEL maintainer="Peter Ruch"

USER root

COPY requirements.txt /requirements.txt

RUN pip3 install --no-cache -U pip setuptools wheel \
    && pip3 install --no-cache -r /requirements.txt

EXPOSE 8080 5555

USER airflow