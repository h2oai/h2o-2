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
echo "Do we have to clean out old ice_root dirs somewhere?"

echo "Setting up sandbox, since no cloud build here will clear it out! (unlike other runners)"
rm -fr sandbox
mkdir -p sandbox

# Should we do this cloud build with the sh2junit.py? to get logging, xml etc.
# I suppose we could just have a test verify the request cloud size, after buildingk
CDH3_JOBTRACKER=192.168.1.176:8021
CDH3_NODES=4
CDH3_HEAP=20g
CDH3_JAR=h2odriver_cdh3.jar

H2O_DOWNLOADED=../../h2o-downloaded
H2O_HADOOP=$H2O_DOWNLOADED/hadoop
H2O_JAR=h2o.jar
HDFS_OUTPUT=hdfsOutputDirName

# file created by the h2o on hadoop h2odriver*jar
REMOTE_HOME=/home/0xcustomer
REMOTE_IP=192.168.1.176
REMOTE_USER=0xcustomer@$REMOTE_IP
REMOTE_SCP="scp -i $HOME/.0xcustomer/0xcustomer_id_rsa"
REMOTE_SSH_USER="ssh -i $HOME/.0xcustomer/0xcustomer_id_rsa $REMOTE_USER"

REMOTE_0XCUSTOMER=0xcustomer@$REMOTE_IP

# have to copy the downloaded h2o stuff over to 176 to execute with the ssh
# it needs the right hadoop client setup. This is easier than installing hadoop client stuff here.
echo "scp some jars"
$REMOTE_SCP $H2O_HADOOP/$CDH3_JAR  $REMOTE_USER:$REMOTE_HOME
$REMOTE_SCP $H2O_DOWNLOADED/$H2O_JAR $REMOTE_USER:$REMOTE_HOME

#***********************************************************************************
echo "Does 0xcustomer have any hadoop jobs left running from something? (manual/jenkins/whatever)"
rm -f /tmp/my_jobs_on_hadoop_$REMOTE_IP

echo "Checking hadoop jobs"
$REMOTE_SSH_USER 'hadoop job -list' > /tmp/my_jobs_on_hadoop_$REMOTE_IP
cat /tmp/my_jobs_on_hadoop_$REMOTE_IP

echo "kill any running hadoop jobs by me"
while read jobid state rest
do
    echo $jobid $state
    # ignore these kind of lines
    # 0 jobs currently running
    # JobId   State   StartTime   UserName    Priority    SchedulingInfo
    if [[ ("$jobid" != "JobId") && ("$state" != "jobs") ]]
    then
        echo "hadoop job -kill $jobid"
        $REMOTE_SSH_USER "hadoop job -kill $jobid"
    fi
done < /tmp/my_jobs_on_hadoop_$REMOTE_IP

#*****HERE' WHERE WE START H2O ON HADOOP*******************************************
rm -f /tmp/h2o_on_hadoop_$REMOTE_IP.sh
echo "cd /home/0xcustomer" > /tmp/h2o_on_hadoop_$REMOTE_IP.sh
echo "rm -fr h2o_one_node" >> /tmp/h2o_on_hadoop_$REMOTE_IP.sh
set +e
# remember to update this, to match whatever user kicks off the h2o on hadoop
echo "hadoop dfs -rmr /user/0xcustomer/$HDFS_OUTPUT" >> /tmp/h2o_on_hadoop_$REMOTE_IP.sh
set -e
echo "hadoop jar $CDH3_JAR water.hadoop.h2odriver -jt $CDH3_JOBTRACKER -libjars $H2O_JAR -mapperXmx $CDH3_HEAP -nodes $CDH3_NODES -output $HDFS_OUTPUT -notify h2o_one_node " >> /tmp/h2o_on_hadoop_$REMOTE_IP.sh
# exchange keys so jenkins can do this?
# background!
cat /tmp/h2o_on_hadoop_$REMOTE_IP.sh
cat /tmp/h2o_on_hadoop_$REMOTE_IP.sh | $REMOTE_SSH_USER &
#*********************************************************************************

CLOUD_PID=$!
jobs -l

echo ""
echo "Have to wait until h2o_one_node is available from the cloud build. Deleted it above."
echo "spin loop here waiting for it."

rm -fr h2o_one_node
while [ ! -f h2o_one_node ]
do
    sleep 5
    set +e
    echo "$REMOTE_SCP $REMOTE_USER:$REMOTE_HOME/h2o_one_node ."
    $REMOTE_SCP $REMOTE_USER:$REMOTE_HOME/h2o_one_node .
    set -e
done
ls -lt h2o_one_node

# use these args when we do Runit
while IFS=';' read CLOUD_IP CLOUD_PORT 
do
    echo $CLOUD_IP, $CLOUD_PORT
done < h2o_one_node

rm -fr h2o-nodes.json
# NOTE: keep this hdfs info in sync with the json used to build the cloud above
../find_cloud.py -f h2o_one_node -hdfs_version cdh3 -hdfs_name_node 192.168.1.176 -expected_size $CDH3_NODES

echo "h2o-nodes.json should now exist"
ls -ltr h2o-nodes.json
# cp it to sandbox? not sure if anything is, for this setup
cp -f h2o-nodes.json sandbox
cp -f h2o_one_node sandbox

#***********************************************************************************

echo "Touch all the 0xcustomer-datasets mnt points, to get autofs to mount them."
echo "Permission rights extend to the top level now, so only 0xcustomer can automount them"
echo "okay to ls the top level here...no secret info..do all the machines hadoop (cdh3) might be using"
for mr in 171 172 173 174 175 176 177 178 179 180
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
DOIT=../testdir_single_jvm/n0.doit

# $DOIT c5/test_c5_KMeans_sphere15_180GB.py || true

# don't run this until we know whether 0xcustomer permissions also exist for the hadoop job
# $DOIT c1/test_c1_rel.py || true
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
echo "The hadoop job(s) should be gone?"
$REMOTE_SSH_USER "hadoop job -list"
