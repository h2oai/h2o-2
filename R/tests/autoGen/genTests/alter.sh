#!/bin/bash/

for f in ./*;
do
    if [[ "$f" =~ ".R" ]];
    then
        sed  '/^[[:space:]]*$/d' $f > out
        mv out $f
    fi
done
