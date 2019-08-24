FROM nginx:alpine

COPY tools/docker/config/airflow-web.conf /etc/nginx/conf.d/
