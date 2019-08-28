#!/bin/bash

DIR="$(dirname "$0")"
ENV_PARTIALS=tools/env_partials

if [ "$DIR" != "scripts" ]; then
    echo "Script has to be called from project root!"
    echo "Navigate to project root and run it again!"
else
    echo "Generating new .env file"
    cp ${ENV_PARTIALS}/.main.env .env

    echo "Generating new .docker.env file"
    cat ${ENV_PARTIALS}/.main.env ${ENV_PARTIALS}/.credentials.env ${ENV_PARTIALS}/.server.env > .docker.env
fi