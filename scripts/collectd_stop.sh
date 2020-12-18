#!/bin/sh

pdsh -w ^/efs/benchmark/workers /efs/benchmark/scripts/collectd_collect.sh
