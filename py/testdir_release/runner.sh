#!/bin/bash

# Ensure that all your children are truly dead when you yourself are killed.
# trap "kill -- -$BASHPID" INT TERM EXIT
# leave out EXIT for now
trap "kill -- -$BASHPID" INT TERM
echo "BASHPID: $BASHPID"
echo "current PID: $$"

source ./runner_setup.sh "$@"

rm -f h2o-nodes.json

if [[ $USER == "jenkins" ]]
then 
    # clean out old ice roots from 0xcust.** (assuming we're going to run as 0xcust..
    # only do this if you're jenksin
    echo "If we use more machines, expand this cleaning list."
    echo "The possibilities should be relatively static over time"
    echo "Could be problems if other threads also using that user on these machines at same time"
    echo "Could make the rm pattern match a "sourcing job", not just 0xcustomer"
    echo "Who cleans up on the target 172-180 machines?"
    
    echo "Also: Touch all the 0xcustomer-datasets mnt points, to get autofs to mount them."
    echo "Permission rights extend to the top level now, so only 0xcustomer can automount them"
    echo "okay to ls the top level here...no secret info..do all the machines we might be using"
    echo ""
    echo "resolve the issue with colliding with other jobs, by only deleting if older than 3 days"

    # 171 dead
    # for mr in 171 172 173 174 175 176 177 178 179 180
    # for mr in 172 173 174 175 176 177 178 179 180
    # only touch the nodes you use?
    for mr in 175 176 177 178 179 180
    do
        ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@172.16.2.$mr  \
            'find /home/0xcustomer/ice* -ctime +3 | xargs rm -rf; cd /mnt/0xcustomer-datasets'
    done

    python ../four_hour_cloud.py -cj pytest_config-jenkins-175-180.json &
else
    if [[ $USER == "kevin" ]]
    then
        # python ../four_hour_cloud.py -cj pytest_config-kevin.json &
        python ../four_hour_cloud.py -cj pytest_config-jenkins-175-180.json &
    else
        python ../four_hour_cloud.py &
    fi
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


echo $TESTDIR
echo $TEST
# avoid for now
# myPy c5 test_c5_KMeans_sphere15_180GB.py
if [[ $TEST == "" ]] || [[ $TESTDIR == "" ]]
then
    # if va and fvec tests are mixed without deleting keys,
    # the import leaves keys that apparently get converted by exec -> timeout
    # just do fvec tests
    # myPy c1 test_c1_rel.py
    # myPy c2 test_c2_fvec.py
    # myPy c3 test_c3_rel.py
    # myPy c4 test_c4_four_billion_rows_fvec.py

    #    myPy c6 test_c6_hdfs_fvec.py
    #    myPy c8 test_c8_rf_airlines_hdfs_fvec.py
    #    myPy c9 test_c9_GLM_airlines_hdfs_fvec.py
    # myPy c10 test_c10_glm_fvec.py
    # myPy c10 test_c10_gbm_fvec.py


    # myPy c9 test_c9_GBM_airlines_hdfs_fvec.py
    # myPy c8 test_c8_rf_airlines_hdfs_fvec.py
    myPy c5 test_c5_KMeans_sphere_26GB_fvec.py
    # dataset is missing
    # myPy c9 test_c9_GLM_rc_fvec.py


    # put known failure last
    # doesn't work. key gets locked. forget about it
    # myPy c7 test_c7_rel.py
else
    myPy $TESTDIR $TEST
fi

# If this one fails, fail this script so the bash dies 
# We don't want to hang waiting for the cloud to terminate.
myPy shutdown test_shutdown.py

if ps -p $CLOUD_PID > /dev/null
then
    echo "$CLOUD_PID is still running after shutdown. Will kill"
    kill $CLOUD_PID
fi
ps aux | grep four_hour_cloud

# test_c2_fvec has about 11 subtests inside it, that will be tracked individually by jenkins
# ../testdir_single_jvm/n0.doit test_c2_fvec || true
# We don't want the jenkins job to complete until we kill it, so the cloud stays alive for debug
# also prevents us from overrunning ourselves with cloud building
# If we don't wait, the cloud will get torn down.

jobs -l
echo ""
echo "You can stop this jenkins job now if you want. It's all done"
