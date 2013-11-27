#!/bin/bash

function error {
    echo ERROR: $@
    echo ERROR: $@ >> results/error.log
}

function info {
    echo INFO: $@
    echo INFO: $@ >> results/info.log
}

curl -s http://localhost:54321 1> /dev/null
if [ $? -ne 0 ]; then
    echo ERROR: could not find h2o at localhost:54321
    exit 1
fi

mkdir results
if [ $? -ne 0 ]; then
    error "directory results already exists"
    exit 1
fi

#curl http://localhost:54321

for test in $(ls */*.R | grep -v Utils)
do
    echo "----------------------------------------------------------------------"
    echo "Starting $test"
    echo "----------------------------------------------------------------------"
    R -f $test 2>&1| tee results/${test}.out
    exit 0
    RC=${PIPESTATUS[0]}
    echo exit code $RC
    if [ $RC -eq 0 ]; then
       info "test PASSED $test"
       echo >> results/${test}.out
       echo PASSED >> results/${test}.out
    else
       info "test FAILED $test with exit code $RC"
       echo >> results/${test}.out
       echo FAILED >> results/${test}.out
    fi
done
