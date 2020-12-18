#!/bin/sh

# stop service
sudo service collectd stop

# aggregate and copy metrics so as not to overwhelm the EFS filesystem
mkdir -p /efs/benchmark/latest/collectd
tar cJf /efs/benchmark/latest/collectd/`hostname -f`.tar.xz -C /var/lib/collectd/csv `hostname -f`
