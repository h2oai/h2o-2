#!/bin/bash

OUTDIR='BigLoggerFiles'
rawLogs=rawLogs

#last 3 digits of inet addr
mach=`ifconfig | grep -o "inet addr:192.168.1.[0-9]*" | grep -o 192.168.1.* | awk -F'.' '{print $4}'`

#log files
cpuPerfFile=${OUTDIR}/`date         +%Y-%m-%d`"-cpuPerf_"$mach".csv"
idlePerfFile=${OUTDIR}/`date        +%Y-%m-%d`"-idlePerf_"$mach".csv"
iowaitPerfFile=${OUTDIR}/`date      +%Y-%m-%d`"-iowaitPerf_"$mach".csv"
memPerfFile=${OUTDIR}/`date         +%Y-%m-%d`"-memPerf_"$mach".csv"
topPerfFile=${OUTDIR}/`date         +%Y-%m-%d`"-topPerf_"$mach".csv"
netReceivePerfFile=${OUTDIR}/`date  +%Y-%m-%d`"-netRPerf_"$mach".csv"
netTransmitPerfFile=${OUTDIR}/`date +%Y-%m-%d`"-netTPerf_"$mach".csv"
swapPerfFile=${OUTDIR}/`date        +%Y-%m-%d`"-sisoPerf_"$mach".csv"

#headers
cpuheader='time(s)'
head -n 33 /proc/stat | awk -F' ' 'OFS="," {print $1}' > tmpfile
cpuheader=$cpuheader,`./transpose.sh tmpfile`
rm tmpfile
memheader='time(s),MemTotal,MemFree,Cached,Writeback,RSS'
topheader='time(s),PID,USER,RES,%CPU,%MEM,COMMAND'
netheader='time(s),dev,bytes,packets,errs,drop'
sisoheader='time(s),si,so'

function checkExists {
    if [ ! -a $1 ]
    then
        touch $1
        echo $2 >> $1
    fi
}

function checkDExists {
 if [ ! -d $1 ]
 then
    mkdir $1
 fi
}

function echoLine {
    if [ $4 -eq 0 ]
    then
        if [ $5 -eq 1 ]
        then
            line=`cat $1`
            echo $(( `date +%s` - $2 )),$6,$line >> $3
        else
            line=`cat $1`
            echo $(( `date +%s` - $2 )),$line >> $3
        fi
    else
        if [ $5 ]
        then
            line=`./transpose.sh $1 | awk -F, 'OFS="," {print $1,$2,$4,$17}'`
            echo $(( `date +%s` - $2 )),$line,$6 >> $3
        else
            line=`./transpose.sh $1`
            echo $(( `date +%s` - $2 )),$line >> $3
        fi
    fi
}

checkDExists ${OUTDIR}
checkDExists ${rawLogs}
checkDExists ${rawLogs}/procstat
checkDExists ${rawLogs}/meminfo
checkDExists ${rawLogs}/netdev
checkDExists ${rawLogs}/vmstat
checkDExists ${rawLogs}/top
checkExists $cpuPerfFile         $cpuheader
checkExists $idlePerfFile        $cpuheader
checkExists $iowaitPerfFile      $cpuheader
checkExists $memPerfFile         $memheader
checkExists $topPerfFile         $topheader
checkExists $netReceivePerfFile  $netheader
checkExists $netTransmitPerfFile $netheader
checkExists $swapPerfFile        $sisoheader

for i in {0..34}
do
    PREVTOTALS[$i]=0
done

start=`cat starttime`
while :; do
    h2oPID=`ps -efww | grep h2o | grep 0xdiag| grep jar|awk '{print $2}' | xargs`
    if [ -z $h2oPID ]
    then
        continue
    fi
    
    #dump raw logs first
    ts=`date +"%Y-%m-%d-%H-%M-%S"`
    cat /proc/stat    >> ${rawLogs}/procstat/${ts}_procstat_${mach}
    cat /proc/meminfo >> ${rawLogs}/meminfo/${ts}_meminfo_${mach}
    cat /proc/net/dev >> ${rawLogs}/netdev/${ts}_netdev_${mach}
    vmstat            >> ${rawLogs}/vmstat/${ts}_vmstat_${mach}
    top -b -n 1       >> ${rawLogs}/top/${ts}_top_${mach}
    a=1
    for i in {0..35}
    do
      TOTALS[$i]=0
    done
    while read -a CPU
    do a=$(($a+1));
      if [ $a -eq 35 ]
      then
          break
      fi
      unset CPU[0]
      CPU=("${CPU[@]}")
      TOTAL=${TOTALS[$a]}
      for t in "${CPU[@]}"; do ((TOTAL+=t)); done
      TOTALS[$a]=$TOTAL
      PREVTOTAL=${PREVTOTALS[$a]}
      ((DIFF_TOTAL=$TOTAL-${PREVTOTAL:-0}))
      OUT_INT_CPU=$((1000*(${CPU[0]}    - ${PREV_CPUS[$a]:-0})/DIFF_TOTAL))
      OUT_INT_IDLE=$((1000*(${CPU[3]}   - ${PREV_IDLES[$a]:-0})/DIFF_TOTAL))
      OUT_INT_IOWAIT=$((1000*(${CPU[4]} - ${PREV_IOWAITS[$a]:-0})/DIFF_TOTAL))
      OUT_CPU[$a]=$((OUT_INT_CPU/10))
      OUT_IDLE[$a]=$((OUT_INT_IDLE/10))
      OUT_IOWAIT[$a]=$((OUT_INT_IOWAIT/10))
      PREV_CPUS[$a]=${CPU[0]}
      PREV_IDLES[$a]=${CPU[3]}
      PREV_IOWAITS[$a]=${CPU[4]}
    done </proc/stat
    PREVTOTALS=( "${TOTALS[@]}" )
    linecpu=`echo "${OUT_CPU[@]}" | tr ' ' ','` 
    lineidle=`echo "${OUT_IDLE[@]}" | tr ' ' ','`
    lineiowait=`echo "${OUT_IOWAIT[@]}" | tr ' ' ','`
    echo $(( `date +%s` - $start )),$linecpu    >> $cpuPerfFile
    echo $(( `date +%s` - $start )),$lineidle   >> $idlePerfFile
    echo $(( `date +%s` - $start )),$lineiowait >> $iowaitPerfFile
    RSS=`ps v  $h2oPID | awk -F' ' 'OFS="," {print $8}' | tail -n 1`
    cat /proc/meminfo      | awk -F' ' 'OFS="," {gsub(":","", $1); print $2}' > bmemTMP
    echo $pwd
    devstat=
    case "$mach" in
     161) devstat="eth1" ;;
     162) devstat="eth2" ;;
     163) devstat="eth3" ;;
     164) devstat="eth3" ;;
    esac
    grep $devstat /proc/net/dev  | awk -F' ' 'OFS="," {print $2,$3,$4,$5}'          > brecTMP
    grep $devstat /proc/net/dev  | awk -F' ' 'OFS="," {print $10,$11,$12,$13}'      > btraTMP
    echoLine bmemTMP $start $memPerfFile         1 1 $RSS
    echoLine brecTMP $start $netReceivePerfFile  0 1 $devstat
    echoLine btraTMP $start $netTransmitPerfFile 0 1 $devstat
    #get top 10 processes from top and then just store them, may/not be interesting...
    ti="$(( `date +%s` - ${start} ))"
    top -b | head -n 17 | tail -n 10 | awk -v t=$ti -F' ' 'OFS="," {print t,$1,$2,$6,$9,$10,$12}' >> $topPerfFile
    vmstat | tail -n 1               | awk -v t=$ti -F' ' 'OFS="," {print t,$7,$8}'               >> $swapPerfFile
    perf stat -x, -e instructions,cycles,cache-references,cache-misses,faults -a -o bTMP -p $h2oPID sleep 10
    rm b*TMP
done
