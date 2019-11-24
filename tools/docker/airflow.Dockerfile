

FROM ml-jku/airflow-core
LABEL maintainer="Peter Ruch"

USER root

COPY requirements.txt /requirements.txt
RUN /opt/conda/bin/pip install --no-cache-dir -r /requirements.txt && \
    /opt/conda/bin/conda clean -ya

EXPOSE 8080 5555

USER airflow