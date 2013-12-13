#!/bin/bash

#globals
pathToH2O="../../target/h2o.jar"
h=3
HEAP="-Xmx"$h"g"
JAVA=`which java`
#computed later
numNodes=
NODES=
NUMTESTS=
BATCHSIZE=

function error {
  echo ERROR: $@
  echo ERROR: $@ >> results/error.log
}

function info {
  echo INFO: $@
  echo INFO: $@ >> results/info.log
}

makeResultsDir() {
  if [ -d results ];
  then
      rm -rf results
  fi
  mkdir results
}

tearDown() {
 ps ax | grep h2o | awk '{print $1}' | xargs kill
}

startJVM() {
  $JAVA $HEAP -jar $pathToH2O -port $1 -name port$1 2&>1 > results/jvmAt$1.out &
  isUp $1
}

runTest() {
  tes=$(basename $1)
  echo "----------------------------------------------------------------------"
    echo "Starting $tes"
    echo "----------------------------------------------------------------------"
    R -f $1 --args localhost:$2 2>&1 > results/$tes.out
    RC=${PIPESTATUS[0]}
    echo exit code $RC
    if [ $RC -eq 0 ]; then
       info "test PASSED $tes"
       echo >> results/$tes.out
       echo PASSED >> results/$tes.out
    else
       info "test FAILED $tes with exit code $RC"
       echo >> results/$tes.out
       echo FAILED >> results/$tes.out
    fi
}

doBatch() {
  node=$1
  batch=${*:2}
  echo "Node is $node"
  for test in $batch; 
  do 
    #echo "$test $node"
    runTest $test $node
  done
}

rm1() {
 if [ -a 1 ];
 then
    rm 1;
 fi
}

isUp() {
  x=`curl -s http://localhost:$1/Cloud.html`
  if [ $? -ne 0 ];
  then
    echo "Retry node $1"
    sleep 2
    isUp $1
  fi
  echo "Node up at http://localhost:$1/"
  return
}

buildCloudNRunTests() {
  makeResultsDir
  base_port=55330
  for i in `seq 1 $1`;
  do
    port=$(( $base_port + 5))
    base_port=$(( $base_port + 5))
    NODES[$i]=$port
    startJVM $port
  done
  unset NODES[0]
  for n in ${NODES[@]}; 
  do 
    echo "Checking that node $n is up"
    isUp $n
  done
  echo "All nodes up! Let's roll!"

  doTasks
  #wait
  rm1
  #tearDown
}

doTasks() {
  NUMTESTS=0
  for test in $(find . | grep -v Utils | grep runit);
  do
    NUMTESTS=$(( $NUMTESTS + 1 ))
  done
  echo "Found $NUMTESTS tests."
  BATCHSIZE=$(( $NUMTESTS / $numNodes ))
  echo "Batching them into batches of size $BATCHSIZE"
  i=0
  nodeNum=1
  for test in $(find . | grep -v Utils | grep runit);
  do
    BATCH[$i]=$test
    i=$(( $i + 1 ))
    if [ $i -eq $BATCHSIZE ];
    then
        doBatch ${NODES[$nodeNum]} ${BATCH[@]} &
        NUMTESTS=$(( $NUMTESTS - $BATCHSIZE ))
        BATCH=
        i=0
        nodeNum=$(( $nodeNum + 1 ))
        if [ $nodeNum -eq $numNodes ];
        then
            BATCHSIZE=$NUMTESTS
        fi
    fi
  done
}
  
main() {
  echo "Specify number of $h GB JVMS to build..."
  echo
  read -p "How many $h GB JVMS do ya want builded, homie? " -n 1 -r
  echo 
  numNodes=$REPLY
  
#  if [[ $REPLY =~ ^[Yy]$ ]]
#  then
  buildCloudNRunTests $REPLY
#  fi
}

echo "Distribute R Tests"
main

