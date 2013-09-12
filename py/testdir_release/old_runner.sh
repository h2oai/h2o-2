#!/bin/bash

# Normally die on first error
set -e

echo "Setting PATH and showing java/python versions"
date
export PATH="/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin"
echo "Checking python/java links and revs first"
echo "JAVA_HOME: $JAVA_HOME"
which java
java -version
which javac
javac -version
echo "PATH: $PATH"
which python
python --version

# This is critical:
# Ensure that all your children are truly dead when you yourself are killed.
trap "kill -- -$BASHPID" INT TERM EXIT
# The -PID argument tells bash to kill the process group with id $BASHPID, 
# Process groups have the same id as the spawning process, 
# The process group id remains even after processes have been reparented. (say by init)
# The -- gets kill not to interpret this as a signal ..
# Don't use kill -9 though to kill this script though!
# Get the latest jar from s3. Has to execute up in h2o

# a secret way to skip the download (use any arg)
if [ $# -eq 0 ]
then
    cd ../..
    ./get_s3_jar.sh
    # I'm back!
    cd -
fi

rm -f h2o-nodes.json
if [[ $USER == "jenkins" ]]
then 
    python ../four_hour_cloud.py -cj ../testdir_hosts/pytest_config-164.json &
else
    python ../four_hour_cloud.py &
fi 

CLOUD_PID=$!
jobs -l

echo ""
echo "Have to wait until h2o-nodes.json is available from the cloud build. Deleted it above."
echo "spin loop here waiting for it. Since the h2o.jar copy slows each node creation"
echo "it might be 12 secs per node"

while [ ! -f ./h2o-nodes.json ]
do
  sleep 5
done
ls -lt ./h2o-nodes.json


# We now have the h2o-nodes.json, that means we started the jvms
# Shouldn't need to wait for h2o cloud here..
# the test should do the normal cloud-stabilize before it does anything.
# n0.doit uses nosetests so the xml gets created on completion. (n0.doit is a single test thing)
# A little '|| true' hack to make sure we don't fail out if this subtest fails
# test_c1_rel has 1 subtest

# This could be a runner, that loops thru a list of tests.

../testdir_single_jvm/n0.doit c1/test_c1_rel.py || true
../testdir_single_jvm/n0.doit c2/test_c2_rel.py || true
# If this one fails, fail this script so the bash dies 
# We don't want to hang waiting for the cloud to terminate.
../testdir_single_jvm/n0.doit test_shutdown.py

kill $CLOUD_PID
ps aux | grep four_hour_cloud

# test_c2_rel has about 11 subtests inside it, that will be tracked individually by jenkins
# ../testdir_single_jvm/n0.doit test_c2_rel || true
# We don't want the jenkins job to complete until we kill it, so the cloud stays alive for debug
# also prevents us from overrunning ourselves with cloud building
# If we don't wait, the cloud will get torn down.

jobs -l
echo ""
echo "You can stop this jenkins job now if you want. It's all done"

if wait $cloud_pid
then
    echo "$cloud_pid succeeded"
else
    echo "$pid failed"
fi

