#!/bin/bash

echo "you can use -n argument to skip the s3 download if you did it once" 
echo "files are unzipped to ../../h2o-downloaded"
# This is critical:
# Ensure that all your children are truly dead when you yourself are killed.
# trap "kill -- -$BASHPID" INT TERM EXIT
# leave out EXIT for now
trap "kill -- -$BASHPID" INT TERM
echo "BASHPID: $BASHPID"
echo "current PID: $$"

source ./runner_setup.sh "$@"
echo "Do we have to clean out old ice_root dirs somewhere?"

echo "Setting up sandbox, since no cloud build here will clear it out! (unlike other runners)"
rm -fr sandbox
mkdir -p sandbox

# Should we do this cloud build with the sh2junit.py? to get logging, xml etc.
# I suppose we could just have a test verify the request cloud size, after buildingk

# resource manager is still on 162
# yarn.resourcemanager.address  8032
CDH5_YARN_JOBTRACKER=192.168.1.180:8032
CDH5_YARN_NODES=1
# FIX! we fail if you ask for two much memory? 7g worked. 8g doesn't work
echo "can't get more than 5g for now. node count 2"
echo "need to adjust the cdh5 cloudera config (yarn memory?)"
CDH5_YARN_HEAP=5g
CDH5_YARN_JAR=h2odriver_cdh4_yarn.jar

H2O_DOWNLOADED=../../h2o-downloaded
H2O_HADOOP=$H2O_DOWNLOADED/hadoop
H2O_JAR=h2o.jar
HDFS_OUTPUT=hdfsOutputDirName

# file created by the h2o on hadoop h2odriver*jar
REMOTE_HOME=/home/0xcustomer
REMOTE_IP=192.168.1.179
REMOTE_USER=0xcustomer@$REMOTE_IP
REMOTE_SCP="scp -i $HOME/.0xcustomer/0xcustomer_id_rsa"
REMOTE_SSH_USER="ssh -i $HOME/.0xcustomer/0xcustomer_id_rsa $REMOTE_USER"

source ./kill_hadoop_jobs.sh

#*****HERE' WHERE WE START H2O ON HADOOP*******************************************
rm -f /tmp/h2o_on_hadoop_$REMOTE_IP.sh
echo "cd /home/0xcustomer" > /tmp/h2o_on_hadoop_$REMOTE_IP.sh
echo "rm -fr h2o_one_node" >> /tmp/h2o_on_hadoop_$REMOTE_IP.sh
set +e
# remember to update this, to match whatever user kicks off the h2o on hadoop
echo "hdfs dfs -rm -r /user/0xcustomer/$HDFS_OUTPUT" >> /tmp/h2o_on_hadoop_$REMOTE_IP.sh
set -e
echo "hadoop jar $CDH5_YARN_JAR water.hadoop.h2odriver -jt $CDH5_YARN_JOBTRACKER -libjars $H2O_JAR -mapperXmx $CDH5_YARN_HEAP -nodes $CDH5_YARN_NODES -output $HDFS_OUTPUT -notify h2o_one_node " >> /tmp/h2o_on_hadoop_$REMOTE_IP.sh

# copy the script, just so we have it there too
$REMOTE_SCP /tmp/h2o_on_hadoop_$REMOTE_IP.sh $REMOTE_USER:$REMOTE_HOME

# have to copy the downloaded h2o stuff over to xxx to execute with the ssh
# it needs the right hadoop client setup. This is easier than installing hadoop client stuff here.
# do the jars last, so we can see the script without waiting for the copy
echo "scp some jars"
$REMOTE_SCP $H2O_HADOOP/$CDH5_YARN_JAR  $REMOTE_USER:$REMOTE_HOME
$REMOTE_SCP $H2O_DOWNLOADED/$H2O_JAR $REMOTE_USER:$REMOTE_HOME

# exchange keys so jenkins can do this?
# background!
cat /tmp/h2o_on_hadoop_$REMOTE_IP.sh
cat /tmp/h2o_on_hadoop_$REMOTE_IP.sh | $REMOTE_SSH_USER &
#*********************************************************************************

CLOUD_PID=$!
jobs -l

source ./wait_for_h2o_on_hadoop.sh

# use these args when we do Runit
while IFS=';' read CLOUD_IP CLOUD_PORT 
do
    echo $CLOUD_IP, $CLOUD_PORT
done < h2o_one_node

rm -fr h2o-nodes.json
# NOTE: keep this hdfs info in sync with the json used to build the cloud above
echo "Make sure you update this to point to the right name node, which can be different than the resource manager"
../find_cloud.py -f h2o_one_node -hdfs_version cdh4_yarn -hdfs_name_node 192.168.1.180 -expected_size $CDH5_YARN_NODES

echo "h2o-nodes.json should now exist"
ls -ltr h2o-nodes.json
# cp it to sandbox? not sure if anything is, for this setup
cp -f h2o-nodes.json sandbox
cp -f h2o_one_node sandbox

#***********************************************************************************

echo "Touch all the 0xcustomer-datasets mnt points, to get autofs to mount them."
echo "Permission rights extend to the top level now, so only 0xcustomer can automount them"
echo "okay to ls the top level here...no secret info..do all the machines hadoop (cdh3) might be using"
for mr in 161 162 163 
do
    ssh -i $HOME/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.$mr 'cd /mnt/0xcustomer-datasets'
done

# We now have the h2o-nodes.json, that means we started the jvms
# Shouldn't need to wait for h2o cloud here..
# the test should do the normal cloud-stabilize before it does anything.
# n0.doit uses nosetests so the xml gets created on completion. (n0.doit is a single test thing)
# A little '|| true' hack to make sure we don't fail out if this subtest fails
# test_c1_rel has 1 subtest
# This could be a runner, that loops thru a list of tests.

# belt and suspenders ..for resolving bucket path names
export H2O_REMOTE_BUCKETS_ROOT=/home/0xcustomer

echo "If it exists, pytest_config-<username>.json in this dir will be used"
echo "i.e. pytest_config-jenkins.json"
echo "Used to run as 0xcust.., with multi-node targets (possibly)"
myPy() {
    DOIT=../testdir_single_jvm/n0.doit
    $DOIT $1/$2 || true
    # try moving all the logs created by this test in sandbox to a subdir to isolate test failures
    # think of h2o.check_sandbox_for_errors()
    rm -f -r sandbox/$1
    mkdir -p sandbox/$1
    cp -f sandbox/*log sandbox/$1
    # rm -f sandbox/*log
}

# myPy c5 test_c5_KMeans_sphere15_180GB.py
# don't run this until we know whether 0xcustomer permissions also exist for the hadoop job
# myPy c1 test_c1_rel.py

myPy c2 test_c2_rel.py
# myPy c3 test_c3_rel.py
# myPy c4 test_c4_four_billion_rows_fvec.py
myPy c6 test_c6_hdfs_fvec.py

# If this one fails, fail this script so the bash dies 
# We don't want to hang waiting for the cloud to terminate.
myPy shutdown test_shutdown.py

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
echo "The mapred job(s) should be gone?"
$REMOTE_SSH_USER "mapred job -list"
