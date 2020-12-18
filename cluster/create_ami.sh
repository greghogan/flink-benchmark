#!/usr/bin/env bash

sudo su
yum-config-manager --enable epel
yum update -y

# http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSPerformance.html
sed -i 's/\(kernel .*\)/\1 xen_blkfront.max=256/' /boot/grub/grub.conf

reboot
sudo su

yum install -y fio collectl ganglia-gmetad ganglia-gmond ganglia-web git htop iftop iotop pdsh \
  sysstat systemtap telnet xfsprogs
stap-prep

# optional: first download then install Oracle JDK
yum localinstall -y jdk-*.rpm && rm -f jdk-*.rpm && rm jdk-*.rpm

# optional: Amazon’s Linux AMI is not kept up-to-date
pip install --upgrade awscli

# install GNU Parallel
(wget -O - pi.dk/3 || curl pi.dk/3/ || fetch -o - pi.dk/3) | bash
rm -rf parallel*

# increase the number of allowed open files and the size of core dumps
cat <<EOF > /etc/security/limits.conf
* soft nofile 1048576
* hard nofile 1048576
* soft core unlimited
* hard core unlimited
EOF

cat <<EOF > /etc/pam.d/common-session
session required pam_limits.so
EOF

# mount and configure EBS volumes during each boot
cat <<EOF >> /etc/rc.local
mkdir -p /volumes
format_and_mount() {
blockdev --setra 512 /dev/xvd\$1
    echo 1024 > /sys/block/xvd\$1/queue/nr_requests
    /sbin/mkfs.ext4 -m 0 /dev/xvd\$1
    mkdir /volumes/xvd\$1
    mount /dev/xvd\$1 /volumes/xvd\$1
    mkdir /volumes/xvd\$1/tmp
    chmod 777 /volumes/xvd\$1/tmp
}
for disk in b c d; do
    format_and_mount \${disk} &
done EOF

sed -i ’s/^PermitRootLogin .*/PermitRootLogin without-password/’ /etc/ssh/sshd_config
service sshd restart

ssh-keygen -N "" -t rsa -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

cat <<EOF > ~/.ssh/config
Host *
    LogLevel ERROR
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
EOF
chmod 600 ~/.ssh/config

rm -rf /tmp/*
> ~/.bash_history && history -c && exit

ssh-keygen -N "" -t rsa -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

cat <<EOF > ~/.ssh/config
Host *
    LogLevel ERROR
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
EOF
chmod 600 ~/.ssh/config

> ~/.bash_history && history -c && exit
