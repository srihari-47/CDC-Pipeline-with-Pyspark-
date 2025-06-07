#!/bin/bash
export SPARK_MASTER_HOST=<tailscale-master-ip>
export SPARK_WORKER_MEMORY=1g
export SPARK_WORKER_CORES=1
export SPARK_DRIVER_MEMORY=512m
export SPARK_DRIVER_CORES=1
export SPARK_DAEMON_MEMORY=256m
export SPARK_DRIVER_HOST=<tailscale ip of node running driver process>
export SPARK_DRIVER_PORT=47100
export SPARK_LOG_DIR=/tmp/spark-logs

