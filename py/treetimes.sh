


# print the last 3 tree ready times from every stdout file in sandbox
# sum the last tree times from each stdout (assume we can sort by time, then take the last, each time 
# the tree # is not greater. Not strictly bulletproof, but should work in practice
# find ./sandbox/*stdout*
find ./sandbox/*stdout* | xargs -ix bash -c "tac x | grep -m 3 -e 'Tree.*ready' | tac" |\
    awk '{if ($2<=lasttwo) {sum=sum + lasttwo; print ""}  lasttwo=$2; print} END {print "\nTree Ready sum is", sum}'

# find ntree just print them all
# modelSize only shows up in the python stdout (doesn't go to log)
cat ./sandbox/commands.log | sed -n \
    -e 's!.*\(ntree.[0-9][0-9]*\).*!\1!p' \



