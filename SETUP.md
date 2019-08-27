# Setup Instructions

This config utilizes encrypted PostgreSQL and RabbitMQ instance. This means we need certificates.
Self-signed suffice for our usecases.

The certificates will be mounted in the Docker containers as [secrets](https://docs.docker.com/engine/swarm/secrets/) via [docker-compose](https://docs.docker.com/compose/compose-file/#secrets).

## Creating Certificate

```bash
mkdir secrets && cd secrets
```

### Self-Signed CA Cert

```bash
openssl req -new -nodes -text -out ca.csr -keyout ca-key.pem -subj "/CN=ca.ml.jku.at"
openssl x509 -req -in ca.csr -text -extfile /etc/ssl/openssl.cnf -extensions v3_ca -signkey ca-key.pem -out ca-cert.pem

chmod 600 *.pem
```

### Generate Certificates for PostgreSQL / RabbitMQ

Run the snippet once for every service.

```bash
#SERVICE=postgres
#SERVICE=rabbitmq

mkdir -p ${SERVICE} && pushd ${SERVICE}
openssl req -new -nodes -text -out server.csr -keyout server-key.pem -subj "/CN=demosite.ml.jku.at"
openssl x509 -req -in server.csr -text -CA ../ca-cert.pem -CAkey ../ca-key.pem -CAcreateserial -out server-cert.pem

openssl req -new -nodes -text -out client.csr -keyout client-key.pem -subj "/CN=${SERVICE}-client"
openssl x509 -req -in client.csr -text -CA ../ca-cert.pem -CAkey ../ca-key.pem -CAcreateserial -out client-cert.pem

chmod 600 *.csr *.pem

mkdir -p /etc/ssl/${SERVICE}
cp ../ca-cert.pem server-cert.pem server-key.pem /etc/ssl/${SERVICE}
chmod -R 700 /etc/ssl/${SERVICE}
chown -R ${SERVICE}:${SERVICE} /etc/ssl/${SERVICE}

popd
```

### Generate Certificate for NGINX

This stop is optional and only applies if you also want to use a self-signed certificate.

```bash
SERVICE=nginx

mkdir -p ${SERVICE} && pushd ${SERVICE}
openssl req -new -nodes -text -out server.csr -keyout server-key.pem -subj "/CN=demosite.ml.jku.at"
openssl x509 -req -in server.csr -text -CA ../ca-cert.pem -CAkey ../ca-key.pem -CAcreateserial -out server-cert.pem

openssl dhparam -out dhparam.pem 4096

chmod 600 *.csr *.pem

popd
```

## Database setup

### Setup airflow PostgreSQL user

```bash
# Login via sudo because PostgresSQL setup is non-standard
sudo -u postgres psql
```

```sql
CREATE USER airflow PASSWORD 'tpp_airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
ALTER ROLE airflow SET search_path = airflow, public;
```

### Update postgresql.conf

```bash
# Ubuntu: /etc/postgresql/10/main/postgresql.conf
# CentOS: /var/lib/pgsql/data/postgresql.conf
listen_addresses = '127.0.0.1,172.18.0.1,140.78.90.110"

ssl = on
ssl_cert_file = '/etc/ssl/postgresql/server-cert.pem'
ssl_key_file = '/etc/ssl/postgresql/server-key.pem'
ssl_ca_file = '/etc/ssl/postgresql/ca-cert.pem'
```

### Update pg_hba.conf

```bash
# Ubuntu: /etc/postgresql/10/main/pg_hba.conf
# CentOS: /var/lib/pgsql/data/pg_hba.conf
#local   airflow         airflow                                 md5
hostssl    all             airflow         172.18.0.0/16           md5  clientcert=1
hostssl    all             airflow         140.78.90.0/16          md5  clientcert=1
```

### Restart PostgreSQL service

```bash
systemctl restart postgresql
```

## RabbitMQ

### Setup airflow RabbitMQ user

```bash
rabbitmqctl add_user airflow tpp_airflow
rabbitmqctl add_vhost airflow
rabbitmqctl set_user_tags airflow airflow
rabbitmqctl set_permissions -p airflow airflow ".*" ".*" ".*"
```

### Create rabbitmq.conf

[Template](https://github.com/rabbitmq/rabbitmq-server/blob/master/docs/rabbitmq.conf.example)

```bash
# Ubuntu: /etc/rabbitmq/rabbitmq.conf
# CentOS: /var/lib/rabbitmq/rabbitmq.conf
listeners.ssl.docker = 172.18.0.1:5671
listeners.ssl.demosite = 140.78.90.110:5671

ssl_options.verify               = verify_peer
ssl_options.fail_if_no_peer_cert = false
ssl_options.cacertfile           = /etc/ssl/rabbitmq/ca-cert.pem
ssl_options.certfile             = /etc/ssl/rabbitmq/server-cert.pem
ssl_options.keyfile              = /etc/ssl/rabbitmq/server-key.pem

management.tcp.port = 15672
management.ssl.port       = 15671
management.ssl.cacertfile = /etc/ssl/rabbitmq/ca-cert.pem
management.ssl.certfile   = /etc/ssl/rabbitmq/server-cert.pem
management.ssl.keyfile    = /etc/ssl/rabbitmq/server-key.pem
```

### Restart RabbitMQ Server service

```bash
systemctl restart rabbitmq-server
```

## Firewall Setup

```bash
sudo ufw allow from 172.18.0.0/16 to any port 5672
```

## Initialize the database

```bash
bash scripts/init_db.sh
```

## Reset the database

```bash
bash scripts/init_db.sh
```

## Environment Settings

Environment settings for the containers running Airflow are stored in `airflow.env`. Docker-compose additionaly needs additional environment variables which are stored in `.env`.

Create a new `.env` file from the `.env.template` template and you are good to go.

```bash
bash scripts/build-containers.sh
```

### Securing Connections

Generate a custom fernet key via Python snippet and add it as `AIRFLOW__CORE__FERNET_KEY=your_fernet_key` to `airflow.env`.

```python
from cryptography.fernet import Fernet
fernet_key= Fernet.generate_key()
print(fernet_key.decode()) # your fernet_key, keep it in secured place!
```

For details see: [Airflow Docs](https://airflow.readthedocs.io/en/stable/howto/secure-connections.html)

### Updateing SQLAlchemy connection strings

The file `airflow.env` contains the config for the docker container. If you want to run airflow without Docker (e.g. for depelopment / debugging) you have to update the connections strings.
The SQLAlchemy connection string is an [RFC-1738-style string`](https://www.urlencoder.io/learn/). More information can be found in the [Airflow Docs](https://airflow.apache.org/howto/connection/postgres.html) on postgres.

You can generate your string using `urllib` in Python. Just update the paths in the snippet below and run it.

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

### Use airflow.env without Docker

You can use `airflow.env` as template for your non-dockerized Airflow setup. You can add the variables to your environment via:

```bash
set -o allexport
source airflow.env
set +o allexport
```
