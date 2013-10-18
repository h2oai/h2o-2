#!/bin/bash

#last 3 digits of inet addr
mach=`ifconfig | grep -o "inet addr:192.168.1.[0-9]*" | grep -o 192.168.1.* | awk -F'.' '{print $4}'`

#log files
cpuPerfFile=`date +%Y-%m-%d`"-cpuPerf_"$mach".csv"
idlePerfFile=`date +%Y-%m-%d`"-idlePerf_"$mach".csv"
iowaitPerfFile=`date +%Y-%m-%d`"-iowaitPerf_"$mach".csv"
memPerfFile=`date +%Y-%m-%d`"-memPerf_"$mach".csv"
topPerfFile=`date +%Y-%m-%d`"-topPerf_"$mach".csv"
netReceivePerfFile=`date +%Y-%m-%d`"-netRPerf_"$mach".csv"
netTransmitPerfFile=`date +%Y-%m-%d`"-netTPerf_"$mach".csv"
swapPerfFile=`date +%Y-%m-%d`"-sisoPerf_"$mach".csv"

#headers
cpuheader='time(s)'
head -n 33 /proc/stat | awk -F' ' 'OFS="," {print $1}' > tmpfile
cpuheader=$cpuheader,`./transpose.sh tmpfile`
rm tmpfile
memheader='time(s),MemTotal,MemFree,Cached,Writeback'
topheader='time(s),PID,USER,RES,%CPU,%MEM,COMMAND'
netheader='time(s),bytes,packets,errs,drop'
sisoheader='time(s),si,so'

function checkExists {
    if [ ! -a $1 ]
    then
        touch $1
        echo $2 >> $1
    fi
}

function echoLine {
    if [ $4 -eq 0 ]
    then
        line=`cat $1`
        echo $(( `date +%s` - $2 )),$line >> $3
    else
        if [ $5 ]
        then
            line=`./transpose.sh $1 | awk -F, 'OFS="," {print $1,$2,$4,$17}'`
            echo $(( `date +%s` - $2 )),$line >> $3
        else
            line=`./transpose.sh $1`
            echo $(( `date +%s` - $2 )),$line >> $3
        fi
    fi
}

checkExists $cpuPerfFile         $cpuheader
checkExists $idlePerfFile        $cpuheader
checkExists $iowaitPerfFile      $cpuheader
checkExists $memPerfFile         $memheader
checkExists $topPerfFile         $topheader
checkExists $netReceivePerfFile  $netheader
checkExists $netTransmitPerfFile $netheader
checkExists $swapPerfFile        $sisoheader

start=`date +%s`
while [  1  ]; do
    cat /proc/stat         | head -n 33      | awk -F' ' 'OFS="," {print $2}' > cpuTMP
    cat /proc/stat         | head -n 33      | awk -F' ' 'OFS="," {print $5}' > idlTMP
    cat /proc/stat         | head -n 33      | awk -F' ' 'OFS="," {print $6}' > iowTMP
    cat /proc/meminfo      | awk -F' ' 'OFS="," {gsub(":","", $1); print $2}' > memTMP
    grep lo /proc/net/dev  | awk -F' ' 'OFS="," {print $2,$3,$4,$5}'          > recTMP
    grep lo /proc/net/dev  | awk -F' ' 'OFS="," {print $10,$11,$12,$13}'      > traTMP
    echoLine cpuTMP $start $cpuPerfFile 1
    echoLine idlTMP $start $idlePerfFile 1
    echoLine iowTMP $start $iowaitPerfFile 1
    echoLine memTMP $start $memPerfFile 1 1
    echoLine recTMP $start $netReceivePerfFile 0 
    echoLine traTMP $start $netTransmitPerfFile 0
    #get top 10 processes from top and then just store them, may/not be interesting...
    ti="$(( `date +%s` - ${start} ))"
    top -b | head -n 17 | tail -n 10 | awk -v t=$ti -F' ' 'OFS="," {print t,$1,$2,$6,$9,$10,$12}' >> $topPerfFile
    vmstat | tail -n 1               | awk -v t=$ti -F' ' 'OFS="," {print t,$7,$8}'               >> $swapPerfFile
    rm *TMP
    sleep 30
done
