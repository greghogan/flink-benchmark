#!/bin/sh

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <parallelism> <scale> [<key> <value> ...]"
    exit 1
fi

mkdir -p /efs/benchmark/latest

./bin/flink run -q -p $1 examples/gelly/flink-gelly-examples_2.11-1.4-SNAPSHOT.jar \
  --algorithm TriangleListing --order undirected \
  --input RMatGraph --simplify undirected --scale $2 --edge_factor 8 \
  --output hash \
  --__job_details_path /efs/benchmark/latest/job.json ${@:3}
