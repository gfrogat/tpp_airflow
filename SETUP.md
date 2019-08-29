# Setup Instructions

This config utilizes encrypted PostgreSQL and RabbitMQ instance. This means we need certificates.
Self-signed suffice for our usecases.

The certificates will be mounted in the Docker containers as [secrets](https://docs.docker.com/engine/swarm/secrets/) via [docker-compose](https://docs.docker.com/compose/compose-file/#secrets).

## Create Docker Subnet

Simply run docker-compose up. It will create the subnet and will fail because the container is missing. We will build the container later.

```bash
docker-compose up
```

## Creating Certificate

```bash
mkdir secrets && cd secrets
```

### Self-Signed CA Cert

Note: `openssl.conf` is located under `/etc/ssl` on Ubuntu.

```bash
SUBJECT_COMMON="\
/C=AT\
/ST=Upper Austria\
/L=Linz\
/O=Institute for Machine Learning\
/OU=Target Prediction Platform"

SUBJECT_DETAIL="\
/CN=ca.ml.jku.at\
/emailAddress=tpp@ml.jku.at"

openssl req -new -nodes -text -out ca.csr -keyout ca-key.pem -subj "${SUBJECT_COMMON}${SUBJECT_DETAIL}"
openssl x509 -req -in ca.csr -text -extfile /etc/pki/tls/openssl.cnf -extensions v3_ca -signkey ca-key.pem -out ca-cert.pem

chmod 600 *.csr *.pem
```

### Generate Certificates for PostgreSQL / RabbitMQ

Run the snippet. We create one server and client certificate and use them with PostgresSQL and RabbitMQ.

```bash
SUBJECT_DETAIL="\
/CN=demosite.ml.jku.at\
/emailAddress=tpp@ml.jku.at"

openssl req -new -nodes -text -out server.csr -keyout server-key.pem -subj "${SUBJECT_COMMON}${SUBJECT_DETAIL}"
openssl x509 -req -in server.csr -text -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem

openssl verify -CAfile ca-cert.pem server-cert.pem

SUBJECT_DETAIL="\
/CN=demosite-client.ml.jku.at\
/emailAddress=tpp@ml.jku.at"

openssl req -new -nodes -text -out client.csr -keyout client-key.pem -subj "${SUBJECT_COMMON}${SUBJECT_DETAIL}"
openssl x509 -req -in client.csr -text -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client-cert.pem

openssl verify -CAfile ca-cert.pem client-cert.pem

chmod 600 *.csr *.pem

#SERVICE=postgres
#SERVICE=rabbitmq

sudo mkdir -p /etc/ssl/${SERVICE}
sudo cp ca-cert.pem server-cert.pem server-key.pem /etc/ssl/${SERVICE}
sudo chmod -R 700 /etc/ssl/${SERVICE}
sudo chown -R ${SERVICE}:${SERVICE} /etc/ssl/${SERVICE}
```

### Generate Certificate for NGINX

Create a separate certificate in case we want to use real certificate for website.

**This stop is optional and only applies if you also want to use a self-signed certificate.**

```bash
SERVICE=nginx

SUBJECT_DETAIL="\
/CN=demosite.ml.jku.at\
/emailAddress=tpp@ml.jku.at"

mkdir -p ${SERVICE} && pushd ${SERVICE}
openssl req -new -nodes -text -out server.csr -keyout server-key.pem -subj "${SUBJECT_COMMON}${SUBJECT_DETAIL}"
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
listen_addresses = '127.0.0.1,172.18.0.1,140.78.90.110'

ssl = on
ssl_cert_file = '/etc/ssl/postgres/server-cert.pem'
ssl_key_file = '/etc/ssl/postgres/server-key.pem'
ssl_ca_file = '/etc/ssl/postgres/ca-cert.pem'
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
sudo rabbitmqctl add_user airflow tpp_airflow
sudo rabbitmqctl add_vhost airflow
sudo rabbitmqctl set_user_tags airflow airflow administrator
sudo rabbitmqctl set_permissions -p airflow airflow ".*" ".*" ".*"
```

### Create rabbitmq.conf

[Template](https://github.com/rabbitmq/rabbitmq-server/blob/master/docs/rabbitmq.conf.example)

This enables access to the management API via HTTP (port 15672) for Celery Flower monitoring as well via HTTPS (port 15671) on localhost.

```bash
# Ubuntu/CentOS: /etc/rabbitmq/rabbitmq.conf
listeners.ssl.docker = 172.18.0.1:5671
listeners.ssl.demosite = 140.78.90.110:5671

ssl_options.verify               = verify_peer
ssl_options.fail_if_no_peer_cert = false
ssl_options.cacertfile           = /etc/ssl/rabbitmq/ca-cert.pem
ssl_options.certfile             = /etc/ssl/rabbitmq/server-cert.pem
ssl_options.keyfile              = /etc/ssl/rabbitmq/server-key.pem

management.tcp.port = 15672
management.tcp.ip = 172.18.0.1
management.ssl.port = 15671
management.ssl.cacertfile = /etc/ssl/rabbitmq/ca-cert.pem
management.ssl.certfile   = /etc/ssl/rabbitmq/server-cert.pem
management.ssl.keyfile    = /etc/ssl/rabbitmq/server-key.pem
```

### Restart RabbitMQ Server service

```bash
# Make sure Docker subnet already exists (docker-compose up)
systemctl stop rabbitmq-server
systemctl start rabbitmq-server
```

## Firewall Setup

```
To      Action      From
--      ------      ----
5432    ALLOW       140.78.90.0/24  # PostgreSQL vom ML Subnet
5432    ALLOW       172.18.0.0/16   # PostgreSQL vom Docker Virtual Network

5671    ALLOW       140.78.90.0/24  # RabbitMQ vom ML Subnet
5671    ALLOW       172.18.0.0/16   # RabbitMQ vom Docker Virtual Network
15672   ALLOW       172.18.0.0/16   # RabbitMQ (api) vom Docker Virtual Network

80,443  ALLOW       140.78.90.0/24  # NGINX Webserver vom ML Subnet
```

## Environment Settings

Environment settings for the containers running Airflow are stored in `.docker.env`. Docker-compose additionaly needs additional environment variables which are stored in `.env`.
You can setup the files running:

```bash
bash scripts/setup_env_files.sh
```

```bash
bash scripts/build-containers.sh
```

If you encounter permission errors chown the keys to 1000:1000 so that they are accessible in the container.

## Initialize the database

```bash
bash scripts/init_db.sh
```

## Reset the database

```bash
#Only in emergency
#bash scripts/reset_db.sh
```

### Securing Connections

Generate a custom fernet key via Python snippet and add it as `AIRFLOW__CORE__FERNET_KEY=your_fernet_key` to `.docker.env`.

```python
from cryptography.fernet import Fernet
fernet_key= Fernet.generate_key()
print(fernet_key.decode()) # your fernet_key, keep it in secured place!
```

For details see: [Airflow Docs](https://airflow.readthedocs.io/en/stable/howto/secure-connections.html)

### Updateing SQLAlchemy connection strings

The file `.docker.env` contains the config for the docker container. If you want to run airflow without Docker (e.g. for depelopment / debugging) you have to update the connections strings.
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

## Running the workers

Note: **This section is temporary and will be updated in the future**

The workers will run directly on the machine in a separate conda environment.

Clone the repository on the worker and setup the `.env` files.

```bash
bash scripts/setup_env_files.sh
```

Open the newly generated file `.conda.env` and update the SQLAlchemy connection string for `POSTGRES_SSL_CONFIG` to use your local SSL certificates (see section above for instructions).

The standard solution is to create a folder `.tpp-credentials` in your home directory and copy the certificates there. You can then use this script to create the connection string. **Important:** you have to double-quote the string so that you can successfully load the environment (due to annoying different parsing behaviour of docker-compose and bash).

```python
# requires > python 3.6
import urllib.parse
from pathlib import Path

secrets_folder = Path("~/.tpp-credentials").expanduser()

params = {
    "sslmode": "verify-ca",
    "sslrootcert": (secrets_folder / "ca-cert.pem").as_posix(),
    "sslkey": (secrets_folder / "client-key.pem").as_posix(),
    "sslcert": (secrets_folder / "client-cert.pem").as_posix(),
}
print(params)

print(urllib.parse.urlencode(params))
```

as well as the `RABBITMQ_SSL_CLIENT_CACERT_*` variables. You can simply copy paste the expanded paths in the params dictionary from the snippet above.

You can source the environment by typing:

````bash
set -o allexport
source .conda.env
printenv
set +o allexport
```

```bash
conda create -n "airflow-worker" python=3.7 -y
conda activate airflow-worker
pip install -r requirements.txt
````
