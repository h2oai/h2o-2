#!/bin/bash

set -e

if [ -z ${AWS_SSH_PRIVATE_KEY_FILE} ]
then
    echo "ERROR: You must set AWS_SSH_PRIVATE_KEY_FILE in the environment."
    exit 1
fi

flatfileName=flatfile.txt
rm -f ${flatfileName}
for privateIp in $(cat nodes-private)
do
    echo ${privateIp}:54321 >> ${flatfileName}
done

for publicDnsName in $(cat nodes-public)
do
    echo "Copying flatfile to ${publicDnsName}"
    scp -o StrictHostKeyChecking=no -i ${AWS_SSH_PRIVATE_KEY_FILE} ${flatfileName} ec2-user@${publicDnsName}:
done

echo Success.
