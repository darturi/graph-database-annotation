#!/bin/bash

DOCKERFILE_DIR="/Users/danielarturi/Desktop/COMP 400/Kuzu1/LDBC_Work"
VOLUME_NAME="age-vs-sql-treebench_age_treebench_data"

# Stop and remove containers
echo "Stopping containers..."
docker compose -f "$DOCKERFILE_DIR/docker-compose.yml" down

# Remove the volume
echo "Removing volume: $VOLUME_NAME"
docker volume rm "$VOLUME_NAME"

# Start containers
echo "Starting containers..."
docker compose -f "$DOCKERFILE_DIR/docker-compose.yml" up -d

echo "Environment reset complete!"
