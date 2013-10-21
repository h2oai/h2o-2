#!/bin/bash

#ssh into each of the machines from ${JSON} (passed as first argument)
MACHINES=`cat BMscripts/$1 | python -c 'import sys, json; print json.load(sys.stdin)[sys.argv[1]]' ip | awk -F, 'OFS="," {gsub("[ u\x27\[\]]","", $0); print}'`
IFS=","
MACHINES=($MACHINES)

function startBigLoggers {
    for i in ${MACHINES[@]}
    do
        echo "Starting bigLogger on ${i}"
        scp BMLogs/starttime spencer@$i:/home/spencer/h2o/bench/BMLogs
        ssh spencer@$i "cd /home/spencer/h2o/bench/BMLogs; bash bigLogger.sh" &
    done
}

function startLittleLoggers {
    for i in ${MACHINES[@]}
    do
        echo "Starting littleLogger on ${i} on phase $1"
        ssh spencer@$i "cd /home/spencer/h2o/bench/BMLogs; bash littleLogger.sh $1" &
    done
}

function stopLittleLoggers {
    for i in ${MACHINES[@]}
    do
        echo "Changing little logger $i to phase $1"
        pids=`ssh spencer@$i ps ax|grep bash|grep littleLogger|awk '{print $1}'| xargs`
        ssh spencer@$i kill $pids
    done
}

function stopAllLoggers {
    for i in ${MACHINES[@]}
    do
        echo "Stopping all loggers on machine $i"
        pids=`ssh spencer@$i ps ax|grep bash|grep Logger|awk '{print $1}'| xargs`
        ssh spencer@$i kill $pids
    done
}

function changePhase {
  stopLittleLoggers $1
  newPhase=$1
  startLittleLoggers $1 >/dev/null
}

function clearCaches {
    for i in ${MACHINES[@]}
    do
        echo "Clearing caches on machine $i"
        ssh spencer@$i ./flushCaches
        ssh 0xdiag@$i rm -rf /home/0xdiag/ice.55555*
    done
}

if [ $2 = "big" ]
then
    startBigLoggers >/dev/null
fi

if [ $2 = "little" ]
then
    startLittleLoggers START >/dev/null
fi

if [ $2 = "changePhase" ]
then
    changePhase $3
fi

if [ $2 = "stop_" ]
then
    stopAllLoggers
fi

if [ $2 = "clear_" ]
then
    clearCaches
fi
