# Target Prediction Pipeline - Airflow Service

This repository contains everything to setup an Apache Airflow Service an a local machine (in our case demosite.ml.jku.at).
The website is currently only available internally in the ML network.

## Configuration

- Apache Airflow
- PostgreSQL Database for Metadata and Result
- RabbitMQ Broker
- Celery Executor
- Encrypted Proby via NGINX to Airflow and Flower webservers for monitoring.

The databases are not dockerized and run on the host as separate services.

To setup everything follow the instructions in [](INSTALL.md) and [](SETUP.md).
