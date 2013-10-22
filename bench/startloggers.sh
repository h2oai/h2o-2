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
        ssh 0xdiag@$i rm -rf /home/0xdiag/ice.55555* /home/0xdiag/*zip
    done
}

function gatherLogs {
    for i in ${MACHINES[@]}
    do
        echo "Gather logs from machine $i"
        mach=`echo $i | awk -F. '{print $4}'`
        if [ ! -d machine_${mach}_logs ]
        then
            mkdir machine_${mach}_logs
        fi
        scp -r spencer@$i:~/h2o/bench/BMLogs/ machine_${mach}_logs
    done
}

function gatherICE {
    phase=$1
    for i in ${MACHINES[@]}
    do
        echo "Gather ICE from machine $i"
        mach=`echo $i | awk -F. '{print $4}'`
        ssh 0xdiag@$i zip -r ice_${phase}_${mach} ice.55555*/h2ologs
        if [ ! -d ICES ]
        then
            mkdir ICES
        fi
        scp 0xdiag@$i:~/ice_${phase}_${mach}.zip ICES/
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

if [ $2 = "gather" ]
then
    gatherLogs >/dev/null
fi

if [ $2 = "ice" ]
then
    gatherICE $3
fi
