#/bin/sh
# Ensure that all your children are truly dead when you yourself are killed.
# http://www.davidpashley.com/articles/writing-robust-shell-scripts/#id2382181
# trap "kill -- -$BASHPID" INT TERM EXIT
# leave out EXIT for now
trap "kill -- -$BASHPID" INT TERM
echo "BASHPID: $BASHPID"
echo "current PID: $$"

set -e

SH2JU=~/shell2junit/sh2ju.sh
echo "Checking that sh2ju.sh exists in the right place"
if [ -f $SH2JU ]
then
    echo "$SH2JU exists."
else
    # http://code.google.com/p/shell2junit
    # how to use in jenkins:
    # http://manolocarrasco.blogspot.com/2010/02/hudson-publish-bach.html
    pushd ~
    wget http://shell2junit.googlecode.com/files/shell2junit-1.0.0.zip
    unzip shell2junit-1.0.0.zip 
    ls -lt shell2junit/sh2ju_example.sh  
    ls -lt shell2junit/sh2ju.sh    
    popd

    if [ -f $SH2JU ]
    then
        echo "$SH2JU exists."
    else
        echo "$SH2JU does not exist. Tried to download it but failed?."
        exit 1
    fi
fi

#### Include the library
source $SH2JU

# This gets the h2o.jar
source ./runner_setup.sh

rm -f h2o-nodes.json
if [[ $USER == "jenkins" ]]
then 
    # clean out old ice roots from 0xcust.** (assuming we're going to run as 0xcust..
    # only do this if you're jenksin
    echo "If we use more machines, expand this cleaning list."
    echo "The possibilities should be relatively static over time"
    echo "Could be problems if other threads also using that user on these machines at same time"
    echo "Could make the rm pattern match a "sourcing job", not just 0xcustomer"
    ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.161 rm -f -r /home/0xcustomer/ice*

    # HACK this is really 161 plus 164. this allows us to talk to localhost:54377 accidently (R)
    python ../four_hour_cloud.py -cj pytest_config-jenkins-161.json &
    CLOUD_IP=192.168.1.161
    CLOUD_PORT=54377
else
    if [[ $USER == "kevin" ]]
    then
        python ../four_hour_cloud.py -cj pytest_config-kevin.json &
        CLOUD_IP=127.0.0.1
        CLOUD_PORT=54355
    else
        python ../four_hour_cloud.py &
        CLOUD_IP=127.0.0.1
        CLOUD_PORT=54321
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

#### Clean old reports
## juLogClean

#******************************************************
# Examples
#******************************************************

#### Success command
juLog  -name=myTrueCommand true || true
#### Failure (just to test that jenkins reports failure)
### juLog  -name=myFalseCommand false || true
#### Sleep
juLog  -name=mySleepCommand sleep 5 || true
#### The test fails because the word 'world' is found in command output
#### (just to test that jenkins reports failure)
#### juLog  -name=myErrorCommand -ierror=world   echo Hello World || true
#### a simple command
juLog  -name=myLsCommand /bin/ls || true

#### A call to a customized method
myCmd() {
    ls -l $*
    return 0
}
juLog  -name=myCustomizedMethod myCmd '*.sh' || true

#******************************************************
# End of Examples
#******************************************************
myRInstall() {
    set +e
    which R
    R --version
    H2O_R_HOME=../../R
    echo "Okay to run h2oWrapper.R every time for now"
    R CMD BATCH $H2O_R_HOME/h2oWrapper-package/R/h2oWrapper.R
}

juLog  -name=myRInstall myRInstall || true

#******************************************************

myR() {
    # these are hardwired in the config json used above for the cloud
    # CLOUD_IP=192.168.1.161
    # CLOUD_PORT=54355

    # get_s3_jar.sh now downloads it. We need to tell anqi's wrapper where to find it.
    # with an environment variable

    which R
    R --version
    H2O_R_HOME=../../R
    H2O_PYTHON_HOME=../../py
    export H2OWrapperDir=$H2O_R_HOME/h2oWrapper-package/R
    echo "H2OWrapperDir env. variable should be $H2OWrapperDir"

    rScript=$H2O_R_HOME/tests/$1
    rLibrary=$H2O_R_HOME/$2
    echo $rScript
    echo $rLibrary
    echo "Running this cmd:"
    echo "R -f $rScript --args $CLOUD_IP:$CLOUD_PORT"
    # don't fail on errors, since we want to check the logs in case that has more info!
    set +e
    R -f $rScript --args $CLOUD_IP:$CLOUD_PORT
    rExitcode=$?
    # check the h2o logs (and R logs) to see if there's any badness! exceptions!
    # this uses the exact same python check_sandbox_for_errors() stuff as python tests
    # so it's always up to date! (it also looks at the R logs if they go to sandbox?
    # where do they go, the way above? I guess we rely on the test causing an exception
    python $H2O_PYTHON_HOME/h2o_sandbox.py
    pExitcode=$?
    set -e
    # exit # status is last command
    exit $rExitcode || $pExitcode
}

# seems like it assumes the regex is present for non-zero exit code cases? (line 92 sh2ju.sh)
# or ???

juLog  -name=runit_RF.R myR 'runit_RF.R' || true
juLog  -name=runit_PCA.R myR 'runit_PCA.R' || true
juLog  -name=runit_kmeans.R myR 'runit_kmeans.R' || true
juLog  -name=runit_GLM.R myR 'runit_GLM.R' || true
juLog  -name=runit_GBM.R myR 'runit_GBM.R' || true

# If this one fails, fail this script so the bash dies 
# We don't want to hang waiting for the cloud to terminate.
../testdir_single_jvm/n0.doit test_shutdown.py

if ps -p $CLOUD_PID > /dev/null
then
    echo "$CLOUD_PID is still running after shutdown. Will kill"
    kill $CLOUD_PID
fi
ps aux | grep four_hour_cloud

jobs -l
echo ""
echo "You can stop this jenkins job now if you want. It's all done"


