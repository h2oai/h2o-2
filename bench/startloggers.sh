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
  stopLittleLoggers $1 2>/dev/null
  newPhase=$1
  startLittleLoggers $1 #2>/dev/null
}

function clearCaches {
    for i in ${MACHINES[@]}
    do
        echo "Clearing caches on machine $i"
        ssh spencer@$i ./flushCaches
        #ssh 0xdiag@$i rm -rf /home/0xdiag/ice.55555* /home/0xdiag/*zip
    done
}

function shredLogs {
    echo "Stop any loggers first..."
    stopAllLoggers 2>/dev/null
    for i in ${MACHINES[@]}
    do  
        echo "Shredding all logs on machine $i"
        ssh spencer@$i rm -rf /home/spencer/h2o/bench/BMLogs/starttime /home/spencer/h2o/bench/BMLogs/BigLogger* /home/spencer/h2o/bench/BMLogs/LittleLogger* /home/spencer/h2o/bench/BMLogs/*TMP
    done
}

function deepClean {
    echo "Stop any loggers first..."
    stopAllLoggers 2>/dev/null
    echo "Clearn all caches..."
    clearCaches
    for i in ${MACHINES[@]}
    do
        echo "Melting ICES on machine $i"
        ssh 0xdiag@$i rm -rf /home/0xdiag/ice.55555* /home/0xdiag/*zip
        echo "Dumping any open instances of h2o..."
        pids1=`ps -efww | grep h2o | grep spencer| grep jar|awk '{print $2}' | xargs`
        ssh spencer@$i kill $pids1
        pids2=`ps -efww | grep h2o | grep 0xdiag| grep jar|awk '{print $2}' | xargs`
        ssh spencer@$i kill $pids2
        echo "Shredding all logs on machine $i"
        ssh spencer@$i rm -rf /home/spencer/h2o/bench/BMLogs/starttime /home/spencer/h2o/bench/BMLogs/BigLogger* /home/spencer/h2o/bench/BMLogs/LittleLogger* /home/spencer/h2o/bench/BMLogs/rawLogs
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
        scp -r spencer@$i:~/h2o/bench/BMLogs/BigLogger*    machine_${mach}_logs/
        scp -r spencer@$i:~/h2o/bench/BMLogs/LittleLogger* machine_${mach}_logs/
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
    stopAllLoggers 2>/dev/null
fi

if [ $2 = "clear_" ]
then
    clearCaches
fi

if [ $2 = "gather" ]
then
    gatherLogs 2>/dev/null
fi

if [ $2 = "ice" ]
then
    gatherICE $3
fi

if [ $2 = "shred" ]
then
    shredLogs
fi

if [ $2 = "deep" ]
then
    echo
    echo "Warning: Deep Clean shreds all logs, forgets all caches, melts all ICE, and dumps any running H2O!!"
    echo
    read -p "Are you sure? " -n 1 -r
    echo 
    if [[ $REPLY =~ ^[Yy]$ ]]
    then
        deepClean 2>/dev/null
    fi
fi
