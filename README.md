# README

## Database setup

```bash
# Login via sudo because PostgresSQL setup is non-standard
sudo -u postgres psql
```

```sql
CREATE USER airflow PASSWORD 'tpp_airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
alter role airflow set search_path = airflow, public;
```

```bash
# Update /etc/postgresql/10/main/pg_hba.conf
# Update /var/lib/pgsql/data/pg_hba.conf
#local   airflow         airflow                                 md5
```

### Docker Setup

Open Postgres to Docker Bridge Subnet

```bash
sudo ufw allow from 172.18.0.0/16 to any port 5432
```

```config
# postgresql.conf
listen_addresses = '127.0.0.1, 172.18.0.1'
```

```config
# pg_hba.conf
host    all             airflow         172.18.0.0/16           md5
```

## Fernet Key

H21rWrmCYKrax24qvFb-K37IbnHCXFfo-67EIIQxeow=

Triggering Task --> only trigger each task once, otherwise error!

## RabbitMQ

```bash
sudo rabbitmqctl add_user airflow tpp_airflow
sudo rabbitmqctl add_vhost airflow
sudo rabbitmqctl set_user_tags airflow airflow
sudo rabbitmqctl set_permissions -p airflow airflow ".*" ".*" ".*"
```

```bash
sudo ufw allow from 172.18.0.0/16 to any port 5672
```
