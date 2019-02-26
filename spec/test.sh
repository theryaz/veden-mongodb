#!/bin/bash
docker stop mongo &>/dev/null && docker rm mongo &>/dev/null

MONGO=$(docker run --name mongo -d mongo:4.0.6)

echo "Running Tests"
docker run --name node --rm --link mongo -v `pwd`:/app -w /app node:8.15.0-alpine /app/node_modules/jasmine/bin/jasmine.js

docker stop mongo &>/dev/null && docker rm mongo &>/dev/null
