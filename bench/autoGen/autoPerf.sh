#!/bin/bash

function mungeG {
    if [ $1 = "net" ]
    then
        IFS=' '
        read -a fs <<< `echo $5/*$1*`
        Rscript $1.R ${fs[0]} ${fs[1]} $2 $3 $4
    else
        Rscript $1.R $5/*$1* $2 $3 $4
    fi
}

function addG {
cat << EOF

<embed src="./svgs/$1_$2_$3.svg" type="image/svg+xml" />
EOF

}


function addMachineStats {
    for m in $1/Logs/*
    do  
        m=${m%*/}; m2=${m##*/};
        machineID=`echo $m2 | awk -F_ '{print $2}'`
        #addMachineDiv $machineID
        count=0
        for s in $m/LittleLoggerFiles/$2/*
        do  
            if [[ "$s" =~ "netT" ]]
            then
                continue
            fi
            if [[ "$s" =~ "cpu" ]]
            then
                mungeG cpu $machineID $2 $1 $m/LittleLoggerFiles/$2
                wait
                #addG cpu $2 $machineID >> perfGraphs.html
                count=$(( $count + 1 ))
            fi
            if [[ "$s" =~ "mem" ]]
            then
                mungeG mem $machineID $2 $1 $m/LittleLoggerFiles/$2
                wait
                #addG mem $2 $machineID >> perfGraphs.html
                count=$(( $count + 1 ))
            fi
            if [[ "$s" =~ "netR" ]]
            then
                mungeG net $machineID $2 $1 $m/LittleLoggerFiles/$2
                wait
                #addG net $2 $machineID >> perfGraphs.html
                count=$(( $count + 1 ))
            fi
            if [[ "$s" =~ "cache" ]]
            then
                mungeG cache $machineID $2 $1 $m/LittleLoggerFiles/$2
                wait
                count=$(( $count + 1 ))
            fi

            if [ $count -eq 4 ] 
            then
                break
            fi  
        done
    done
}


for dir in ./benchRoot/*
do
    dir=${dir%*/}
    #makeTab $d
    for f in $dir/benchmarks/*
    do  
        svg=`echo $f| awk -F'/' '{print $NF}' | awk -F'bench.csv' '{print $1}'`
        #addTable ${svg}_${d} $f $svg
        addMachineStats $dir $svg
    done
    #endDiv2
done
