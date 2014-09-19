#!/bin/bash
# ssh -i ~/.ec2/keys/mrjenkins_test.pem root@ec2-184-73-55-110.compute-1.amazonaws.com

# good
ssh -i ~/.ec2/keys/mrjenkins_test.pem hduser@23.21.237.69

# ssh -i ~/.ec2/keys/0xdata_Big.pem ubuntu@ec2-23-23-182-217.compute-1.amazonaws.com
# this one is for getting to the jenkins master machine
# ssh -i ~/.ec2/keys/0xdata_Big.pem ubuntu@23.23.182.217
