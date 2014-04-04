#!/bin/bash

# Normally die on first error
set -e

# This is critical:
# Ensure that all your children are truly dead when you yourself are killed.
# trap "kill -- -$BASHPID" INT TERM EXIT
# leave out EXIT for now
trap "kill -- -$$" INT TERM
echo "current PID: $$"

#**************************************
# do some bash parameters, just in case we have future expansion
# -n is no download of the jar
NO_DOWNLOAD=0
USE_EXISTING=0
BRANCH=master
VERSION=
TESTDIR=
TEST=
while getopts v:hunb:d:t: flag
do
    case $flag in
        v)
            VERSION=$OPTARG
            echo "version is $VERSION"
            ;;
        u)
            echo "Use existing target/h2o.jar and R stuff?"
            USE_EXISTING=1
            NO_DOWNLOAD=1
            ;;
        n)
            echo "Won't download the h2o.jar from S3. Assume target/h2o.jar exists"
            NO_DOWNLOAD=1
            ;;
        b)
            BRANCH=$OPTARG
            echo "branch is $BRANCH"
            ;;
        d)
            TESTDIR=$OPTARG
            echo "testdir is $TESTDIR"
            ;;
        t)
            TEST=$OPTARG
            echo "test is $TEST"
            ;;
        h)
            echo "-u Use existing target/h2o.jar, target/R/*  and existing downloaded hadoop drivers"
            echo "-n Init target/* from existing download of h2o stuff from s3 (h2o-downloaded)."
            echo "-b <BRANCH> Use this branch for any s3 download"
            echo "-v <version> Use this version within a branch for any s3 download"
            echo "-d <dir> -t <python test> will run a single test"
            exit
            ;;
        ?)
            echo "Something wrong with the args to runner_setup.sh"
            exit
            ;;
    esac
done
shift $(( OPTIND - 1 ))  # shift past the last flag or argument
# echo remaining parameters to Bash are $*

echo "using branch: $BRANCH"
echo "using version: $VERSION"

#**************************************

# The -PID argument tells bash to kill the process group with id $$, 
# Process groups have the same id as the spawning process, 
# The process group id remains even after processes have been reparented. (say by init)
# The -- gets kill not to interpret this as a signal ..
# Don't use kill -9 though to kill this script though!
# Get the latest jar from s3. Has to execute up in h2o

# a secret way to skip the download (use any arg)
if [ $NO_DOWNLOAD -eq 0 ]
then
    cd ../..
    if [[ $VERSION -eq "" ]]
    then
        ./get_s3_jar.sh -b $BRANCH
    else
        ./get_s3_jar.sh -b $BRANCH -v $VERSION
    fi
    # I'm back!
    cd -
fi

#**************************************
echo "Setting PATH and showing java/python versions"
date
export PATH="/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin"
echo "Checking python/java links and revs first"
# JAVA_HOME might not exist. no need to check?
# echo "JAVA_HOME: $JAVA_HOME"
which java
java -version
which javac
javac -version
echo "PATH: $PATH"
which python
python --version
