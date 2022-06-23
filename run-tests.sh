#!/bin/bash

## required for M1
#export DOCKER_BUILDKIT=1
#export DOCKER_DEFAULT_PLATFORM=linux/amd64 

echo "Going down..."
docker-compose -f docker-compose.yml -f docker-compose.tests.yml down

echo "Pulling harder..."
docker-compose -f docker-compose.yml -f docker-compose.tests.yml pull

echo "Building stronger..."
docker-compose -f docker-compose.yml -f docker-compose.tests.yml build

echo "Running faster..."
docker-compose -f docker-compose.yml -f docker-compose.tests.yml up --no-build --remove-orphans