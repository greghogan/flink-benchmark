#!/bin/sh

# install collectd
sudo pdsh -w ^/efs/benchmark/workers yum -y install collectd

# update collectd configuration
sudo pdsh -w ^/efs/benchmark/workers /bin/cp /efs/benchmark/scripts/collectd.conf /etc/

# remove existing files
sudo pdsh -w ^/efs/benchmark/workers /bin/rm -rf /var/lib/collectd/csv

# start service
sudo pdsh -w ^/efs/benchmark/workers service collectd start
