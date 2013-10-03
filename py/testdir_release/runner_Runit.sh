#/bin/sh

# takes a -n argument to disable the s3 download for faster testing

# Ensure that all your children are truly dead when you yourself are killed.
# http://www.davidpashley.com/articles/writing-robust-shell-scripts/#id2382181
# trap "kill -- -$BASHPID" INT TERM EXIT
# leave out EXIT for now
trap "kill -- -$BASHPID" INT TERM
echo "BASHPID: $BASHPID"
echo "current PID: $$"

set -o pipefail  # trace ERR through pipes
set -o errtrace  # trace ERR through 'time command' and other functions
set -o nounset   ## set -u : exit the script if you try to use an uninitialised variable
set -o errexit   ## set -e : exit the script if any statement returns a non-true return value

# remove any test*xml or TEST*xml in the current dir
rm -f test.*xml

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
    echo "Also: Touch all the 0xcustomer-datasets mnt points, to get autofs to mount them."
    echo "Permission rights extend to the top level now, so only 0xcustomer can automount them"
    echo "okay to ls the top level here...no secret info..do all the machines we might be using"

    for mr in 161 164
    do
        ssh -i ~/.0xcustomer/0xcustomer_id_rsa 0xcustomer@192.168.1.$mr  \
            'echo rm -f -r /home/0xcustomer/ice*; cd /mnt/0xcustomer-datasets'
    done

    # HACK this is really 161 plus 164. this allows us to talk to localhost:54377 accidently (R)
    # python ../four_hour_cloud.py -cj pytest_config-jenkins-161.json &
    # CLOUD_IP=192.168.1.161
    python ../four_hour_cloud.py -cj pytest_config-jenkins.json &
    # make sure this matches what's in the json!
    CLOUD_IP=192.168.1.164
    CLOUD_PORT=54355
else
    if [[ $USER == "kevin" ]]
    then
        python ../four_hour_cloud.py -cj pytest_config-kevin.json &
        # make sure this matches what's in the json!
        CLOUD_IP=127.0.0.1
        CLOUD_PORT=54355
    else
        python ../four_hour_cloud.py &
        # make sure this matches what the four_hour_cloud.py does!
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

#******************************************************
mySetup() {
    # we setup .Renviron and delete the old local library if it exists
    # then make the R_LIB_USERS dir
    which R
    R --version
    # don't always remove..other users may have stuff he doesn't want to re-install
    if [[ $USER == "jenkins" ]]
    then 
        echo "Rebuilding ~/.Renviron and ~/.Rprofile for $USER"
        # Set CRAN mirror to a default location
        rm -f ~/.Renviron
        rm -f ~/.Rprofile
        echo "options(repos = \"http://cran.stat.ucla.edu\")" > ~/.Rprofile
        echo "R_LIBS_USER=\"~/.Rlibrary\"" > ~/.Renviron
        rm -f -r ~/.Rlibrary
        mkdir -p ~/.Rlibrary
    fi

    # removing .Rlibrary should have removed h2oWrapper
    # but maybe it was installed in another library (site library)
    # make sure it's removed, so the install installs the new (latest) one
    cat <<!  > /tmp/libPaths.cmd
    .libPaths()
    myPackages = rownames(installed.packages())
    if("h2oWrapper" %in% myPackages) {
      remove.packages("h2oWrapper")
    }
!

    cmd="R -f /tmp/libPaths.cmd --args $CLOUD_IP:$CLOUD_PORT"
    echo "Running this cmd:"
    echo $cmd
    # everything after -- is positional. grabbed by argparse.REMAINDER
    ./sh2junit.py -name $1 -timeout 30 -- $cmd
}

myR() {
    # these are hardwired in the config json used above for the cloud
    # CLOUD_IP=
    # CLOUD_PORT=
    # get_s3_jar.sh now downloads it. We need to tell anqi's wrapper where to find it.
    # with an environment variable
    if [ -z "$2" ] 
    then
        timeout=30 # default to 30
    else
        timeout=$2
    fi

    which R
    R --version
    H2O_R_HOME=../../R
    H2O_PYTHON_HOME=../../py

    # first test will cause an install
    # this is where we downloaded to. 
    # notice no version number
    # ../../h2o-1.6.0.1/R/h2oWrapper_1.0.tar.gz
    export H2OWrapperDir=../../h2o-downloaded/R
    echo "H2OWrapperDir should be $H2OWrapperDir"
    ls $H2OWrapperDir/h2oWrapper*.tar.gz

    # we want $1 used for -name below, to not have .R suffix
    rScript=$H2O_R_HOME/tests/$1.R
    echo $rScript
    echo "Running this cmd:"
    cmd="R -f $rScript --args $CLOUD_IP:$CLOUD_PORT"
    echo $cmd

    # don't fail on errors, since we want to check the logs in case that has more info!
    set +e
    # everything after -- is positional. grabbed by argparse.REMAINDER
    ./sh2junit.py -name $1 -timeout $timeout -- $cmd || true
    set -e
}

H2O_R_HOME=../../R
echo "Okay to run h2oWrapper.R every time for now"

#***********************************************************************
# This is the list of tests
#***********************************************************************
mySetup libPaths

# can be slow if it had to reinstall all packages?
export H2OWrapperDir=../../h2o-downloaded/R
echo "Showing the H2OWrapperDir env. variable. Is it .../../h2o-downloaded/R?"
printenv | grep H2OWrapperDir
myR runit_RF 120
myR runit_PCA 35
myR runit_GLM 35
myR runit_kmeans 60
myR runit_tail_numeric.R 60
myR runit_summary_numeric.R 60
myR runit_GBM 1200
# If this one fals, fail this script so the bash dies 
# We don't want to hang waiting for the cloud to terminate.
# produces xml too!
../testdir_single_jvm/n0.doit test_shutdown.py

#***********************************************************************
# End of list of tests
#***********************************************************************

if ps -p $CLOUD_PID > /dev/null
then
    echo "$CLOUD_PID is still running after shutdown. Will kill"
    kill $CLOUD_PID
fi
ps aux | grep four_hour_cloud

jobs -l
echo ""
echo "You can stop this jenkins job now if you want. It's all done"

