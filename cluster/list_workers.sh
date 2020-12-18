#!/usr/bin/env bash

# save the list of workers - could also filter on instance type;
# remove master node from list of IPs if also captured by the filter
aws ec2 describe-instances --filter Name=placement-group-name,Values=greg-pg | \
  python -c $'import json, sys; print "\\n".join(i["PrivateIpAddress"] for r in \
  json.load(sys.stdin)["Reservations"] for i in r["Instances"] \
  if i["State"]["Name"] == "running")' > /efs/benchmark/workers && wc /efs/benchmark/workers
