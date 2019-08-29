# Frequently asked questions / problems

## I cannot connect to the PostgreSQL database

If you re-created the docker network via docker-compose the databases likely don't listen on the correct network interfaces. Check the logs for errors!

Restarting both PostgreSQL and RabbitMQ-Server usually resolves the issue.

```bash
sudo systemctl restart postgresql
sudo systemctl restart rabbitmq-server
```

## Mobile Safari (i.e. iOS Browsers) slow loading time

You'll have to import the CA cert into iOS. See this [SO Question](https://stackoverflow.com/questions/35624332/site-on-self-signed-cert-loads-slowly-on-mobile-safari)
