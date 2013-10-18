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
no_download=0
while getopts nf: flag
do
    case $flag in
        n)
            echo "Won't download the h2o.jar from S3. Assume target/h2o.jar exists"
            no_download=1
            ;;
        f)
            file=$OPTARG
            echo "filename is $file (fake)"
            ;;
        ?)
            exit
            ;;
    esac
done
shift $(( OPTIND - 1 ))  # shift past the last flag or argument
echo remaining parameters to Bash are $*

#**************************************

# The -PID argument tells bash to kill the process group with id $$, 
# Process groups have the same id as the spawning process, 
# The process group id remains even after processes have been reparented. (say by init)
# The -- gets kill not to interpret this as a signal ..
# Don't use kill -9 though to kill this script though!
# Get the latest jar from s3. Has to execute up in h2o

# a secret way to skip the download (use any arg)
if [ $no_download -eq 0 ]
then
    cd ../..
    ./get_s3_jar.sh
    # I'm back!
    cd -
fi

#**************************************
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
