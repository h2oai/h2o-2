#!/bin/bash

set -e

if [ -z ${AWS_SSH_PRIVATE_KEY_FILE} ]
then
    echo "ERROR: You must set AWS_SSH_PRIVATE_KEY_FILE in the environment."
    exit 1
fi

for publicDnsName in $(cat nodes-public)
do
    echo Starting on ${publicDnsName}...
    ssh -o StrictHostKeyChecking=no -i ${AWS_SSH_PRIVATE_KEY_FILE} ec2-user@${publicDnsName} ./start-h2o-bg.sh
done

echo Success.
