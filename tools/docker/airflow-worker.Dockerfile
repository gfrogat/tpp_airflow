FROM ml-jku/airflow-core
LABEL maintainer="Peter Ruch"

ARG SECRETS_DIR=/run/secrets
ARG TPP_CREDENTIALS="./tpp-credentials"
ARG PYSPARK_PYTHON="/system/user/ruch/miniconda3/envs/airflow-worker"

USER root

RUN apt-get update -y && apt-get install -y \
    --no-install-recommends \
    xorg \
    && rm -rf /var/lib/apt/lists/*

#RUN curl -o ~/miniconda.sh -O  https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh  && \
     #chmod +x ~/miniconda.sh && \
     #~/miniconda.sh -b -p /opt/conda && \
     #rm ~/miniconda.sh && \
     #/opt/conda/bin/conda install -y python=3.7 openjdk && \
     #/opt/conda/bin/conda install -c mordred-descriptor -c rdkit mordred && \
     #/opt/conda/bin/conda clean -ya && \
     #/opt/conda/bin/pip install --no-cache-dir -r /requirements.txt

#COPY third_party/tpp_python /opt/tpp_python
#RUN /opt/conda/bin/pip install --no-cache-dir /opt/tpp_python

ENV PATH ${PYSPARK_PYTHON}/bin:${PATH}

COPY ${TPP_CREDENTIALS}/ca-cert.pem ${SECRETS_DIR}/ca-cert
COPY ${TPP_CREDENTIALS}/client-key.pem ${SECRETS_DIR}/postgres-client-key
COPY ${TPP_CREDENTIALS}/client-cert.pem ${SECRETS_DIR}/postgres-client-cert
COPY ${TPP_CREDENTIALS}/client-key.pem ${SECRETS_DIR}/rabbitmq-client-key
COPY ${TPP_CREDENTIALS}/client-cert.pem ${SECRETS_DIR}/rabbitmq-client-cert

RUN chown -R airflow:airflow ${SECRETS_DIR}

USER airflow