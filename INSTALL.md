# Workflow Server

## Setup

### Docker + Docker-Compose

Install [Docker](https://docs.docker.com/install/linux/docker-ce/centos/) and [Docker Compose](https://docs.docker.com/compose/install/) for CentOS.

### PostgresSQL

```bash
sudo yum install postgresql-server postgresql-contrib

sudo postgresql-setup initdb
sudo systemctl start postgresql
sudo systemctl enable postgresql

sudo passwd postgres
```

### RabbitMQ

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

### Security

#### Self signed certificates

##### CA Cert

```bash
openssl req -new -nodes -text -out ca.csr -keyout ca-key.pem -subj "/CN=ca.ml.jku.at"
openssl x509 -req -in ca.csr -text -extfile /etc/ssl/openssl.cnf -extensions v3_ca -signkey ca-key.pem -out ca-cert.pem
```

##### SSL - Postgres

```bash
mkdir -p postgres && pushd postgres
openssl req -new -nodes -text -out server.csr -keyout server-key.pem -subj "/CN=igne"
openssl x509 -req -in server.csr -text -CA ../ca-cert.pem -CAkey ../ca-key.pem -CAcreateserial -out server-cert.pem

openssl req -new -nodes -text -out client.csr -keyout client-key.pem -subj "/CN=postgres-client"
openssl x509 -req -in client.csr -text -CA ../ca-cert.pem -CAkey ../ca-key.pem -CAcreateserial -out client-cert.pem
popd
```

##### SSL - RabbitMQ

```bash
mkdir -p rabbitmq && pushd rabbitmq
openssl req -new -nodes -text -out server.csr -keyout server-key.pem -subj "/CN=igne"
openssl x509 -req -in server.csr -text -CA ../ca-cert.pem -CAkey ../ca-key.pem -CAcreateserial -out server-cert.pem

openssl req -new -nodes -text -out client.csr -keyout client-key.pem -subj "/CN=rabbitmq-client"
openssl x509 -req -in client.csr -text -CA ../ca-cert.pem -CAkey ../ca-key.pem -CAcreateserial -out client-cert.pem
popd
```

##### SSL - Nginx

```bash
mkdir -p nginx && pushd nginx
openssl req -new -nodes -text -out server.csr -keyout server-key.pem -subj "/CN=igne"
openssl x509 -req -in server.csr -text -CA ../ca-cert.pem -CAkey ../ca-key.pem -CAcreateserial -out server-cert.pem
popd
```

```bash
openssl dhparam -out /etc/nginx/dhparam.pem 4096
```

##### Copy keys to folders

```bash
sudo mkdir -p /etc/ssl/nginx/
sudo cp ../ca-cert.pem server-cert.pem server-key.pem /etc/ssl/nginx/
sudo chmod -R 700 /etc/ssl/nginx/
# your "nginx user" ngingx or www-data
sudo chown -R www-data:www-data /etc/ssl/nginx/
```

#### Generate sqlalchemy connection string for postgres

String is an `RFC-1738-style string`. [Airflow Docs](https://airflow.apache.org/howto/connection/postgres.html)

```python
import urllib.parse

params = {
    "sslmode": "verify-ca",
    "sslrootcert": "/run/secrets/ca-cert",
    "sslkey": "/run/secrets/postgres-client-key",
    "sslcert": "/run/secrets/postgres-client-cert",
}

urllib.parse.urlencode(params)
```
