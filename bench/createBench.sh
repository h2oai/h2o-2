#!/bin/bash

#This script gathers all of the necessary csv files for the html generator to use.
#Arguments: branch_name config_json


#The structure it creates is this:

#Root:
#    benchmarks
#        *.csv
#    Logs
#        overall
#            *cpuPerf*
#            *memPerf*
#            ...
#        phases
#            *phasePCA*
#            *phaseKMeans*
#            ...

function makeStructure {
    if [ -d $1 ]
    then
        rm -rf $1
    fi
    root=$1
    mkdir -p ${root}/benchmarks
    mkdir -p ${root}/Logs
}



h2obuild=`cat latest`
branch=$1
DATE=`date +%Y-%m-%d`
root=${branch}_${h2obuild}
makeStructure ${root}

#move the actual benchmarks to the benchRoot
cp benchmarks/${h2obuild}/${DATE}/*csv ${root}/benchmarks
wait

#gather up machine logs
bash startloggers.sh $2 gather >/dev/null
wait
mv mach* ${root}/Logs
