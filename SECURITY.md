# Securing the databases

## Self signed certificates

```bash
openssl req -new -nodes -text -out ca.csr -keyout ca-key.pem -subj "/CN=ca.ml.jku.at"
openssl x509 -req -in ca.csr -text -extfile /etc/ssl/openssl.cnf -extensions v3_ca -signkey ca-key.pem -out ca-cert.pem
```

```bash
openssl req -new -nodes -text -out server.csr -keyout server-key.pem -subj "/CN=postgres-server"
openssl x509 -req -in server.csr -text -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem

openssl req -new -nodes -text -out client.csr -keyout client-key.pem -subj "/CN=postgres-client"
openssl x509 -req -in client.csr -text -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client-cert.pem
```

```bash
openssl req -new -nodes -text -out server.csr -keyout server-key.pem -subj "/CN=rabbitmq-server"
openssl x509 -req -in server.csr -text -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem

openssl req -new -nodes -text -out client.csr -keyout client-key.pem -subj "/CN=rabbitmq-client"
openssl x509 -req -in client.csr -text -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client-cert.pem
```
