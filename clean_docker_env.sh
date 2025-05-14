#!/bin/bash

# Stop all running containers
if [ "$(docker ps -q)" ]; then
  docker stop $(docker ps -q)
fi

# Remove all containers
if [ "$(docker ps -a -q)" ]; then
  docker rm -f $(docker ps -a -q)
fi

# Remove all volumes
if [ "$(docker volume ls -q)" ]; then
  docker volume rm $(docker volume ls -q)
fi

# Remove all removable networks
docker network prune -f

# Remove all images
if [ "$(docker images -a -q)" ]; then
  docker rmi -f $(docker images -a -q)
fi

df -h

echo "Docker environment has been cleaned up."

