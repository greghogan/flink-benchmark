#!/usr/bin/env bash

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <number of nodes>"
    exit 1
fi

INSTANCE_TYPE=c4.8xlarge
SPOT_PRICE="0.75"

AVAILABILITY_ZONE=us-east-1a
PLACEMENT_GROUP=greg-pg
SUBNET_ID=

AMI=
EFS_ID_AND_REGION=
KEY_NAME=greg
SECURITY_GROUP_ID=greg-sg

# "--wrap=0" option not available in BSD flavors of base64
USER_DATA=$(base64 <<EOF
#!/bin/bash
mkdir /efs && mount -t nfs4 -o nfsvers=4.1 ${AVAILABILITY_ZONE}.${EFS_ID_AND_REGION}.amazonaws.com:/ /efs
EOF
) | tr -d '\r\n'

LAUNCH_SPECIFICATION=$(cat <<EOF
{
  "ImageId": "${AMI}",
  "KeyName": "${KEY_NAME}",
  "UserData": "${USER_DATA}",
  "InstanceType": "${INSTANCE_TYPE}",
  "Placement": {
    "AvailabilityZone": "${AVAILABILITY_ZONE}",
    "GroupName": "${PLACEMENT_GROUP}"
  },
  "BlockDeviceMappings": [
    { "DeviceName": "/dev/sdb",
      "Ebs": { "VolumeSize": 200, "DeleteOnTermination": true, "VolumeType": "gp2", "Encrypted": true } },
    { "DeviceName": "/dev/sdc",
      "Ebs": { "VolumeSize": 200, "DeleteOnTermination": true, "VolumeType": "gp2", "Encrypted": true } },
    { "DeviceName": "/dev/sdd",
      "Ebs": { "VolumeSize": 200, "DeleteOnTermination": true, "VolumeType": "gp2", "Encrypted": true } },
    { "DeviceName": "/dev/sde",
      "Ebs": { "VolumeSize": 200, "DeleteOnTermination": true, "VolumeType": "gp2", "Encrypted": true } },
    { "DeviceName": "/dev/sdf",
      "Ebs": { "VolumeSize": 200, "DeleteOnTermination": true, "VolumeType": "gp2", "Encrypted": true } }
  ],
"SubnetId": "${SUBNET_ID}",
  "EbsOptimized": true,
  "SecurityGroupIds": [ "${SECURITY_GROUP_ID}" ]
}
EOF
)

INSTANCE_COUNT=$1
aws ec2 request-spot-instances \
  --spot-price $SPOT_PRICE \
  --instance-count $INSTANCE_COUNT \
  --type "one-time" \
  --launch-specification "${LAUNCH_SPECIFICATION}"
