#!/bin/bash
echo "You can use -n argument to skip the s3 download if you did it once" 
echo "files were unzipped to ../../h2o-downloaded"
echo "UPDATE: currently using whatever got built locally. no copy"

# Ensure your child processes are truly dead when you are killed.
# trap "kill -- -$BASHPID" INT TERM EXIT
# leave out EXIT for now
trap "kill -- -$BASHPID" INT TERM
echo "BASHPID: $BASHPID"
echo "current PID: $$"

echo "Do we have to clean out old ice_root dirs somewhere? hadoop. so no?"
echo "Setting up sandbox, since no cloud build here will clear it out! (unlike other runners)"
rm -fr sandbox
mkdir -p sandbox

SET_JAVA_HOME="export JAVA_HOME=/usr/lib/jvm/java-7-oracle; "
# alternately could use this
# SET_JAVA_HOME="export JAVA_HOME=/usr/bin/java"

# Should we do this cloud build with the sh2junit.py? to get logging, xml etc.
# I suppose we could just have a test verify the request cloud size, after building
# Now resource manager is at 8050?
NAME_NODE=172.16.6.11
HDP_JOBTRACKER=172.16.6.11:8050

HDP_NODES=4
HDP_HEAP=4g

# HDP_JAR=h2odriver_hdp2.0.6.jar
HDP_JAR=h2odriver_hdp2.1.jar
H2O_JAR=h2o.jar

# build.sh removes the h2odriver stuff a 'make' creates
# if we do 'make', we shouldn't have to use the downloaded
H2O_DOWNLOADED=../../h2o-downloaded
H2O_BUILT=../../target

source ./runner_setup.sh "$@"

# NOTE: using local build, not downloaded
# HDP_JAR_USED=$H2O_DOWNLOADED/hadoop/$HDP_JAR
# HDP_JAR_USED=$H2O_DOWNLOADED/$HDP_JAR
H2O_JAR_USED=$H2O_BUILT/$H2O_JAR
HDP_JAR_USED=$H2O_BUILT/hadoop/$HDP_JAR

HDFS_OUTPUT=hdfsOutputDirName

REMOTE_HOME=/home/0xcustomer
REMOTE_IP=172.16.6.187
REMOTE_USER=0xcustomer@$REMOTE_IP
REMOTE_SCP="scp -p -i $HOME/.0xcustomer/0xcustomer_id_rsa "

# I shouldn't have to specify JAVA_HOME for a non-iteractive shell running the hadoop command?
# but not getting it otherwise on these machines
# REMOTE_SSH_USER="ssh -i $HOME/.0xcustomer/0xcustomer_id_rsa $REMOTE_USER"
REMOTE_SSH_USER="ssh -i $HOME/.0xcustomer/0xcustomer_id_rsa $REMOTE_USER"
# can't use this with -i 
REMOTE_SSH_USER_WITH_JAVA="$REMOTE_SSH_USER $SET_JAVA_HOME"

# source ./kill_hadoop_jobs.sh

#*****HERE' WHERE WE START H2O ON HADOOP*******************************************
rm -f /tmp/h2o_on_hadoop_$REMOTE_IP.sh
echo "$SET_JAVA_HOME" > /tmp/h2o_on_hadoop_$REMOTE_IP.sh; chmod 777 /tmp/h2o_on_hadoop_$REMOTE_IP.sh
echo "cd /home/0xcustomer" >> /tmp/h2o_on_hadoop_$REMOTE_IP.sh
# h2o_one_node is the file created by the h2odriver
echo "rm -fr h2o_one_node" >> /tmp/h2o_on_hadoop_$REMOTE_IP.sh
set +e
# remember to update this, to match whatever user kicks off the h2o on hadoop
echo "hadoop dfs -rmr /user/0xcustomer/$HDFS_OUTPUT" >> /tmp/h2o_on_hadoop_$REMOTE_IP.sh
chmod +x /tmp/h2o_on_hadoop_$REMOTE_IP.sh
set -e

echo "port: start looking at 55821. Don't conflict with jenkins using all sorts of ports starting at 54321 (it can multiple jobs..so can use 8*10 or so port)"

# EA=" -ea"
# ApiWatchdog fail if I use -ea?
EA=""
# FIX! how to pass -random_udp_drop thru h2odriver?
# Limit cores to see what happens
# h2o checks that it's >=4
# does this has to be greater than the # of jvms? it's a total, not a per jvm total?
# THREADS=" -nthreads 8"
# THREADS=" -nthreads 200"
DROP=" -random_udp_drop"
echo "hadoop jar $HDP_JAR water.hadoop.h2odriver -jt $HDP_JOBTRACKER -libjars $H2O_JAR -baseport 55821 -mapperXmx $HDP_HEAP -nodes $HDP_NODES -output $HDFS_OUTPUT -notify h2o_one_node $EA $DROP" >> /tmp/h2o_on_hadoop_$REMOTE_IP.sh

# Usage: h2odriver
#           -libjars <.../h2o.jar>
#           [other generic Hadoop ToolRunner options]
#           [-h | -help]
#           [-jobname <name of job in jobtracker (defaults to: 'H2O_nnnnn')>]
#               (Note nnnnn is chosen randomly to produce a unique name)
#           [-driverif <ip address of mapper->driver callback interface>]
#           [-driverport <port of mapper->driver callback interface>]
#           [-network <IPv4network1Specification>[,<IPv4network2Specification> ...]
#           [-timeout <seconds>]
#           [-disown]
#           [-notify <notification file name>]
#           -mapperXmx <per mapper Java Xmx heap size>
#           [-extramempercent <0 to 20>]
#           -n | -nodes <number of H2O nodes (i.e. mappers) to create>
#           [-nthreads <maximum typical worker threads, i.e. cpus to use>]
#           [-baseport <starting HTTP port for H2O nodes; default is 54321>]
#           [-ea]
#           [-verbose:gc]
#           [-XX:+PrintGCDetails]
#           [-license <license file name (local filesystem, not hdfs)>]
#           -o | -output <hdfs output dir>

# copy the script, just so we have it there too
$REMOTE_SCP /tmp/h2o_on_hadoop_$REMOTE_IP.sh $REMOTE_USER:$REMOTE_HOME

# Have to copy the downloaded h2o stuff over to xxx to execute with the ssh
# Actually now, this script is using the local build in target (R and h2o.jar)

# it needs the right hadoop client setup. This is easier than installing hadoop client stuff here.
# do the jars last, so we can see the script without waiting for the copy
echo "scp the downloaded h2o driver jar over to the remote machine"
echo "scp the h2o jar we're going to use over to the remote machine"
echo "WARNING: using the local build, not the downloaded h2odriver jar"
$REMOTE_SCP $HDP_JAR_USED $REMOTE_USER:$REMOTE_HOME
$REMOTE_SCP $H2O_JAR_USED $REMOTE_USER:$REMOTE_HOME

# exchange keys so jenkins can do this?
# background ..we could just ssh the file we copied over there?
cat /tmp/h2o_on_hadoop_$REMOTE_IP.sh
cat /tmp/h2o_on_hadoop_$REMOTE_IP.sh | $REMOTE_SSH_USER &
#*********************************************************************************

echo "check on jobs I backgrounded locally"
CLOUD_PID=$!
jobs -l

rm -f h2o_one_node # local copy
source ./wait_for_h2o_on_hadoop.sh

# use these args when we do Runit
while IFS=';' read CLOUD_IP CLOUD_PORT 
do
    echo $CLOUD_IP, $CLOUD_PORT
done < h2o_one_node

rm -fr h2o-nodes.json
# NOTE: keep this hdfs info in sync with the json used to build the cloud above
../find_cloud.py -f h2o_one_node -hdfs_version hdp2.1 -hdfs_name_node $NAME_NODE -expected_size $HDP_NODES

echo "h2o-nodes.json should now exist"
ls -ltr h2o-nodes.json
# cp it to sandbox? not sure if anything is, for this setup
# sandbox might not exist?
cp -f h2o-nodes.json sandbox
cp -f h2o_one_node sandbox

#***********************************************************************************
# 
# echo "Touch all the 0xcustomer-datasets mnt points, to get autofs to mount them."
# echo "Permission rights extend to the top level now, so only 0xcustomer can automount them"
# echo "okay to ls the top level here...no secret info..do all the machines hadoop (cdh4) might be using"
# for mr in 181 182 183 184 185 186 187 188 189 190
# do
#     ssh -i $HOME/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.6.$mr 'cd /mnt/0xcustomer-datasets'
# done

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
    # sandbox log copying to special dir per test is done in n0.doit
}

# don't run this until we know whether 0xcustomer permissions also exist for the hadoop job
# myPy c1 test_c1_rel.py

# worked
myPy c2 test_c2_fvec.py
myPy c6 test_c6_hdfs_fvec.py
myPy c9 test_c9_GLM_airlines_hdfs_multi.py

# myPy c3 test_c3_rel.py
# test_c8_rf_airlines_hdfs_fvec.py
# test_c4_four_billion_rows_fvec.py
# myPy c5 test_c5_KMeans_sphere_h1m_fvec.py

# GOOD!
myPy c5 test_c5_KMeans_sphere_26GB_fvec.py

# myPy c5 test_c5_KMeans_sphere15_180GB_fvec.py

# have to update this to poit to the right hdfs?

# If this one fails, fail this script so the bash dies 
# We don't want to hang waiting for the cloud to terminate.
myPy shutdown test_shutdown.py

# echo "Maybe it takes some time for hadoop to shut it down? sleep 10"
sleep 10
if ps -p $CLOUD_PID > /dev/null
then
    echo "$CLOUD_PID is still running after shutdown. Will kill"
    kill $CLOUD_PID
    # may take a second?
    sleep 1
fi

echo ""
echo "Check if the background ssh/hadoop/cloud job is still running here"
# don't exit code 1 if no match on the grep
ps aux | grep 0xcustomer_id_rsa | grep -v 'grep' || /bin/true
echo "check on jobs I backgrounded locally"
jobs -l

echo ""
echo "Check if h2odriver is running on the remote machine."
$REMOTE_SSH_USER "ps aux | grep h2odriver | grep -v 'grep || /bin/true'"

echo "The background job with the remote ssh that does h2odriver should be gone. It was pid $CLOUD_PID"
echo "The h2odriver job should be gone. It was pid $CLOUD_PID"
echo "The hadoop job(s) should be gone?"
$REMOTE_SSH_USER_WITH_JAVA 'mapred job -list'

# echo "Another hack because h2o nodes don't seem to want to shutdown"
# jecho "Maybe I need a clean terminate of the background h2odriver? But why aren't all nodes processing sent h2o shutdown requests?"
# ./kill_0xcustomer_hadoop_jobs_on_187.sh 
