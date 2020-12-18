#!/bin/sh

if [ "$#" -lt 3 ]; then
    echo "Usage: $0 <parallelism> <scale> <edge factor> [<key> <value> ...]"
    exit 1
fi

mkdir -p /efs/benchmark/graph_metrics

./bin/flink run -q -p $1 examples/gelly/flink-gelly-examples_2.11-1.4-SNAPSHOT.jar \
  --algorithm GraphMetrics --order directed \
  --input RMatGraph --simplify directed --scale $2 --edge_factor $3 \
  --output hash \
  --__job_details_path /efs/benchmark/graph_metrics/directed_s${2}e${3}.json ${@:4} \
  | tee /efs/benchmark/graph_metrics/directed_s${2}e${3}.out
