# may need to 
#       easy_install boto 
# if you didn't already
# The AWS* environment variables need to be set (in your .bashrc?)
# you can get them from files in your ~/.ec2 probably, since you need them there 
# for running test stuff usually..i.e. good to have .ec2 in your home everywhere
from pprint import pprint
from boto import ec2
import os

AWS_ACCESS_KEY_ID = 'XXXXXXXXXXXXXXXXXX'
AWS_SECRET_ACCESS_KEY = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'


def check_required_env_variables():
    ok = True
    if not os.environ['AWS_ACCESS_KEY_ID']:
        warn("AWS_ACCESS_KEY_ID need to be defined!")
        ok = False
    if not os.environ['AWS_SECRET_ACCESS_KEY']:
        warn("AWS_SECRET_ACCESS_KEY need to be defined!")
        ok = False

    if not ok: raise Exception("\033[91m[ec2] Missing AWS environment variables!\033[0m")


check_required_env_variables()
ec2conn = ec2.connection.EC2Connection(os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'])
reservations = ec2conn.get_all_instances()
instances = [i for r in reservations for i in r.instances]
for i in instances:
    print "\n"
    pprint(i.__dict__)
print len(instances), "instances at aws"
