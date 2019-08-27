# Installation Instructions

## Docker + Docker-Compose

Install [Docker](https://docs.docker.com/install/linux/docker-ce/centos/) and [Docker Compose](https://docs.docker.com/compose/install/) for CentOS.

## PostgresSQL

```bash
sudo yum install postgresql-server postgresql-contrib

sudo postgresql-setup initdb
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

## RabbitMQ

```bash
# Follow instructions
# https://packagecloud.io/rabbitmq/erlang/install#manual-rpm
sudo yum install erlang-21.3.8.6

# Follow instructions
# https://packagecloud.io/rabbitmq/rabbitmq-server/install#manual-rpm
sudo yum install rabbitmq-server-3.7.17

# Install Versionlock to be safe
yum install yum-plugin-versionlock

# Lock erlang, rabbitmq and postgres
sudo yum versionlock erlang-*
sudo yum versionlock rabbitmq-server-*

# Start rabbitmq and enable at boot
sudo systemctl start rabbitmq-server.service
sudo systemctl enable rabbitmq-server.service
```
