#!/bin/bash

set -e

# Adjust based on the build of H2O you want to download.
h2oBranch=master
h2oBuild=1036
h2oVersion=1.7.0.${h2oBuild}

if [ -z ${AWS_SSH_PRIVATE_KEY_FILE} ]
then
    echo "ERROR: You must set AWS_SSH_PRIVATE_KEY_FILE in the environment."
    exit 1
fi

for publicDnsName in $(cat nodes-public)
do
    echo "Downloading h2o.jar to ${publicDnsName}"
    ssh -o StrictHostKeyChecking=no -i ${AWS_SSH_PRIVATE_KEY_FILE} ec2-user@${publicDnsName} wget https://s3.amazonaws.com/h2o-release/h2o/${h2oBranch}/${h2oBuild}/h2o-${h2oVersion}.zip
    ssh -o StrictHostKeyChecking=no -i ${AWS_SSH_PRIVATE_KEY_FILE} ec2-user@${publicDnsName} unzip h2o-${h2oVersion}.zip
    ssh -o StrictHostKeyChecking=no -i ${AWS_SSH_PRIVATE_KEY_FILE} ec2-user@${publicDnsName} cp h2o-${h2oVersion}/h2o.jar .
done

echo Success.
