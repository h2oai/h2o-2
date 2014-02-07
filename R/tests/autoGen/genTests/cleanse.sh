#!/bin/bash/

for f in ./*;
do
    if [[ "$f" =~ ".R" ]];
    then
        lc=`wc -l $f | awk '{print $1}'`
        if [[ $lc -lt 11 || $lc -gt 300 ]];
        then
            rm $f
        fi
    fi
done
    
