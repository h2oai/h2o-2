#!/bin/bash

echo you can use -n argument to skip the s3 download if you did it once 
echo files are unzipped to ../../h2o-downloaded
# This is critical:
# Ensure that all your children are truly dead when you yourself are killed.
# trap "kill -- -$BASHPID" INT TERM EXIT
# leave out EXIT for now
trap "kill -- -$BASHPID" INT TERM
echo "BASHPID: $BASHPID"
echo "current PID: $$"

source ./runner_setup.sh

rm -f h2o-nodes.json
echo "Do we have to clean out old ice_root dirs somewhere?"

echo "Setting up sandbox, since no cloud build here will clear it out! (unlike other runners)"
rm -fr sandbox
mkdir -p sandbox

# Should we do this cloud build with the sh2junit.py? to get logging, xml etc.
# I suppose we could just have a test verify the request cloud size, after buildingk
CDH3_JOBTRACKER=192.168.1.176:8021
CDH3_NODES=4
CDH3_HEAP=20g
H2O_DOWNLOADED=../../h2o-downloaded
H2O_HADOOP=$H2O_DOWNLOADED/hadoop
H2O_JAR=$H2O_DOWNLOADED/h2o.jar
HDFS_OUTPUT=hdfsOutputDirName

# file created by the h2o on hadoop h2odriver*jar
H2O_ONE_NODE=h2o_one_node

# have to copy the downloaded h2o stuff over to 176 to execute with the ssh
# it needs the right hadoop client setup. This is easier than installing hadoop client stuff here.
scp -i ~/.0xdiag/0xdiag_id_rsa $H2O_HADOOP/h2odriver_cdh3.jar  0xdiag@192.168.1.176:/home/0xdiag
scp -i ~/.0xdiag/0xdiag_id_rsa $H2O_JAR  0xdiag@192.168.1.176:/home/0xdiag

rm -f /tmp/ssh_to_176.sh
echo "cd /home/0xdiag" > /tmp/ssh_to_176.sh
echo "rm -fr $H2O_ONE_NODE" >> /tmp/ssh_to_176.sh
set +e
# remember to update this, to match whatever user kicks off the h2o on hadoop
echo "hadoop dfs -rmr /user/0xdiag/$HDFS_OUTPUT" >> /tmp/ssh_to_176.sh
set -e
echo "hadoop jar h2odriver_cdh3.jar water.hadoop.h2odriver -jt $CDH3_JOBTRACKER -libjars h2o.jar -mapperXmx $CDH3_HEAP -nodes $CDH3_NODES -output $HDFS_OUTPUT -notify $H2O_ONE_NODE " >> /tmp/ssh_to_176.sh
# exchange keys so jenkins can do this?
# background!
cat /tmp/ssh_to_176.sh | ssh -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.176  &

CLOUD_PID=$!
jobs -l

echo ""
echo "Have to wait until $H2O_ONE_NODE is available from the cloud build. Deleted it above."
echo "spin loop here waiting for it."

rm -fr $H2O_ONE_NODE
while [ ! -f ./$H2O_ONE_NODE ]
do
    sleep 5
    set +e
    scp -i ~/.0xdiag/0xdiag_id_rsa 0xdiag@192.168.1.176:/home/0xdiag/$H2O_ONE_NODE .
    set -e
done
ls -lt ./$H2O_ONE_NODE

# use these args when we do Runit
while IFS=';' read CLOUD_IP CLOUD_PORT 
do
    echo $CLOUD_IP, $CLOUD_PORT
done < ./$H2O_ONE_NODE

rm -fr h2o-nodes.json
../find_cloud.py -f $H2O_ONE_NODE

echo "h2o-nodes.json should now exist"
ls -ltr h2o-nodes.json
# cp it to sandbox? not sure if anything is, for this setup
cp -f h2o-nodes.json sandbox
cp -f $H2O_ONE_NODE sandbox

# We now have the h2o-nodes.json, that means we started the jvms
# Shouldn't need to wait for h2o cloud here..
# the test should do the normal cloud-stabilize before it does anything.
# n0.doit uses nosetests so the xml gets created on completion. (n0.doit is a single test thing)
# A little '|| true' hack to make sure we don't fail out if this subtest fails
# test_c1_rel has 1 subtest

# This could be a runner, that loops thru a list of tests.

echo "If it exists, pytest_config-<username>.json in this dir will be used"
echo "i.e. pytest_config-jenkins.json"
echo "Used to run as 0xcust.., with multi-node targets (possibly)"
DOIT=../testdir_single_jvm/n0.doit

# $DOIT c5/test_c5_KMeans_sphere15_180GB.py || true
$DOIT c1/test_c1_rel.py || true
$DOIT c2/test_c2_rel.py || true
# $DOIT c3/test_c3_rel.py || true
# $DOIT c4/test_c4_four_billion_rows.py || true
$DOIT c6/test_c6_hdfs.py || true

# If this one fails, fail this script so the bash dies 
# We don't want to hang waiting for the cloud to terminate.
../testdir_single_jvm/n0.doit test_shutdown.py

echo "Maybe it takes some time for hadoop to shut it down? sleep 10"
sleep 10
if ps -p $CLOUD_PID > /dev/null
then
    echo "$CLOUD_PID is still running after shutdown. Will kill"
    kill $CLOUD_PID
    # may take a second?
    sleep 1
fi
ps aux | grep h2odriver

jobs -l
echo ""
echo "The h2odriver job should be gone. It was pid $CLOUD_PID"
# 
