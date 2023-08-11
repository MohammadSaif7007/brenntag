#!/bin/bash

set -e

mc alias set minio http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD} 
mc alias list
mc mb minio/demo-data --ignore-existing
mc mb minio/dal --ignore-existing
mc mb minio/temp --ignore-existing
mc cp /data/Customers.csv minio/demo-data/
mc cp /data/orders.json minio/demo-data/