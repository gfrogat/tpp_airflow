FROM ml-jku/airflow-core
LABEL maintainer="Peter Ruch"

ARG SECRETS_DIR=/run/secrets
ARG TPP_CREDENTIALS="./worker-secrets"

USER root

RUN apt-get update -y && apt-get install -y \
    --no-install-recommends \
    xorg \
    openjdk-8-jdk \
    openssh-server \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN wget https://archive.apache.org/dist/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz && \
    tar xzf spark-2.4.3-bin-hadoop2.7.tgz -C /opt && \
    wget http://mirror.klaus-uwe.me/apache/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz && \
    tar xzf hadoop-2.7.7.tar.gz -C /opt

ENV SPARK_HOME /opt/spark-2.4.3-bin-hadoop2.7
ENV HADOOP_HOME /opt/hadoop-2.7.7
ENV LD_LIBRARY_PATH ${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH}

RUN /opt/conda/bin/conda install -c mordred-descriptor -c rdkit mordred && \
    /opt/conda/bin/conda clean -ya

COPY requirements.txt /requirements.txt
COPY ./third_party/tpp_python /opt/tpp_python
RUN /opt/conda/bin/pip install --no-cache-dir -r /requirements.txt && \
    /opt/conda/bin/pip install --no-cache-dir /opt/tpp_python

COPY ${TPP_CREDENTIALS}/ca-cert.pem ${SECRETS_DIR}/ca-cert
COPY ${TPP_CREDENTIALS}/client-key.pem ${SECRETS_DIR}/postgres-client-key
COPY ${TPP_CREDENTIALS}/client-cert.pem ${SECRETS_DIR}/postgres-client-cert
COPY ${TPP_CREDENTIALS}/client-key.pem ${SECRETS_DIR}/rabbitmq-client-key
COPY ${TPP_CREDENTIALS}/client-cert.pem ${SECRETS_DIR}/rabbitmq-client-cert

RUN chown -R airflow:airflow ${SECRETS_DIR}

COPY tools/docker/scripts/start_spark.sh /start_spark.sh
RUN chmod +x /start_spark.sh

EXPOSE 7077 4040 8080

USER airflow