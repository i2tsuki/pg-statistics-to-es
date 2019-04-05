#!/bin/sh

set -eu

docker build --pull -t pg-statistics-to-es .
CONTAINER_ID=$(docker create pg-statistics-to-es)
docker cp ${CONTAINER_ID}:/pg-statistics-to-es/dist ./
docker rm ${CONTAINER_ID}
