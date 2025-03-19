#!/bin/sh
set -e

mc config host add myminio http://minio:9000 minioadmin minioadmin
mc mb myminio/warehouse || echo "Bucket already exists"