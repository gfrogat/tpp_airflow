#!/bin/bash

case "$1" in
  webserver)
    exec airflow webserver
    ;;
  scheduler)
    exec airflow "$@"
    ;;
  flower)
    exec airflow "$@"
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    exec "$@"
    ;;
esac