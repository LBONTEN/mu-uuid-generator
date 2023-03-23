#!/bin/sh

imageTag="lennybontenakel/mu-uuid-generator:latest"
docker build -t ${imageTag} . && docker push ${imageTag}