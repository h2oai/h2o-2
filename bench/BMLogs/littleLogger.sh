#!/bin/bash

OUTDIR='LittleLoggerFiles'

if [ ! -d ${OUTDIR} ]
then
    mkdir ${OUTDIR}
fi

if [ ! -d ${OUTDIR}/$1 ]
then
    mkdir ${OUTDIR}/$1
fi

#last 3 digits of inet addr
mach=`ifconfig | grep -o "inet addr:192.168.1.[0-9]*" | grep -o 192.168.1.* | awk -F'.' '{print $4}'`

#log files
cpuPerfFile=${OUTDIR}/$1/$1-`date         +%Y-%m-%d`"-cpuPerf_"$mach".csv"
idlePerfFile=${OUTDIR}/$1/$1-`date        +%Y-%m-%d`"-idlePerf_"$mach".csv"
iowaitPerfFile=${OUTDIR}/$1/$1-`date      +%Y-%m-%d`"-iowaitPerf_"$mach".csv"
memPerfFile=${OUTDIR}/$1/$1-`date         +%Y-%m-%d`"-memPerf_"$mach".csv"
topPerfFile=${OUTDIR}/$1/$1-`date         +%Y-%m-%d`"-topPerf_"$mach".csv"
netReceivePerfFile=${OUTDIR}/$1/$1-`date  +%Y-%m-%d`"-netRPerf_"$mach".csv"
netTransmitPerfFile=${OUTDIR}/$1/$1-`date +%Y-%m-%d`"-netTPerf_"$mach".csv"
swapPerfFile=${OUTDIR}/$1/$1-`date        +%Y-%m-%d`"-sisoPerf_"$mach".csv"
cachePerfFile=${OUTDIR}/$1/$1-`date       +%Y-%m-%d`"-cachePerf_"$mach".csv"

#headers
cpuheader='time(s)'
head -n 33 /proc/stat | awk -F' ' 'OFS="," {print $1}' > tmpfile
cpuheader=$cpuheader,`./transpose.sh tmpfile`

if [ -a tmpfile ]
then
    rm tmpfile
fi

memheader='time(s),MemTotal,MemFree,Cached,Writeback,RSS'
topheader='time(s),PID,USER,RES,%CPU,%MEM,COMMAND'
netheader='time(s),dev,bytes,packets,errs,drop'
sisoheader='time(s),si,so'
cacheheader='time(s),instructions,cycles,cache-references,cache-misses,faults'

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
        if [ $5 -eq 1 ]
        then
            line=`cat $1`
            echo $(( `date +%s` - $2 )),$6,$line >> $3
        else
            line=`cat $1`
            echo $(( `date +%s` - $2 )),$line >> $3
        fi
    else
        if [ $5 -eq 1 ]
        then
            line=`./transpose.sh $1 | awk -F, 'OFS="," {print $1,$2,$4,$17}'`
            echo $(( `date +%s` - $2 )),$line,$6 >> $3
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
checkExists $cachePerfFile       $cacheheader

for i in {0..35}
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
    #RSS -> ps v  25707 | awk -F' ' 'OFS="," {print $8}'
    RSS=`ps v  $h2oPID | awk -F' ' 'OFS="," {print $8}' | tail -n 1`
    cat /proc/meminfo      | awk -F' ' 'OFS="," {gsub(":","", $1); print $2}' > lmemTMP
    devstat=
    case "$mach" in
     161) devstat="eth1" ;;
     162) devstat="eth2" ;;
     163) devstat="eth3" ;;
     164) devstat="eth3" ;;
    esac
    grep $devstat /proc/net/dev  | awk -F' ' 'OFS="," {print $2,$3,$4,$5}'          > lrecTMP
    grep $devstat /proc/net/dev  | awk -F' ' 'OFS="," {print $10,$11,$12,$13}'      > ltraTMP

    echoLine lmemTMP $start $memPerfFile         1 1 $RSS
    echoLine lrecTMP $start $netReceivePerfFile  0 1 $devstat
    echoLine ltraTMP $start $netTransmitPerfFile 0 1 $devstat
    #get top 10 processes from top and then just store them, may/not be interesting...
    ti="$(( `date +%s` - ${start} ))"
    top -b | head -n 17 | tail -n 10 | awk -v t=$ti -F' ' 'OFS="," {print t,$1,$2,$6,$9,$10,$12}' >> $topPerfFile
    vmstat | tail -n 1               | awk -v t=$ti -F' ' 'OFS="," {print t,$7,$8}'               >> $swapPerfFile
    perf stat -x, -e instructions,cycles,cache-references,cache-misses,faults -a -o lTMP -p $h2oPID sleep 10
    
    tail -n 5 lTMP > lcacheTMP
    ./transpose.sh lcacheTMP | head -n 1 >> debug_out
    line=`./transpose.sh lcacheTMP | head -n 1`
    echo $(( `date +%s` - ${start})),$line >> $cachePerfFile
    
    rm l*TMP
done


