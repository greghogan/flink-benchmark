#!/bin/sh

mkdir -p /efs/benchmark/latest/

python -u ./statsd_server.py 9020 > /efs/benchmark/latest/statsd_server.log
