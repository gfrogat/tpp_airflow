FROM ml-jku/airflow:latest
LABEL maintainer="Peter Ruch"

ARG SECRETS_DIR=/run/secrets
ARG TPP_CREDENTIALS="./tpp-credentials"

USER root

COPY ${TPP_CREDENTIALS}/ca-cert.pem ${SECRETS_DIR}/ca-cert
COPY ${TPP_CREDENTIALS}/client-key.pem ${SECRETS_DIR}/postgres-client-key
COPY ${TPP_CREDENTIALS}/client-cert.pem ${SECRETS_DIR}/postgres-client-cert
COPY ${TPP_CREDENTIALS}/client-key.pem ${SECRETS_DIR}/rabbitmq-client-key
COPY ${TPP_CREDENTIALS}/client-cert.pem ${SECRETS_DIR}/rabbitmq-client-cert

RUN chown -R airflow:airflow ${SECRETS_DIR}

USER airflow