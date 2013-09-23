#!/bin/bash

set -e

d=`dirname $0`

# Use 90% of RAM for H2O.
memTotalKb=`cat /proc/meminfo | grep MemTotal | sed 's/MemTotal:[ \t]*//' | sed 's/ kB//'`
memTotalMb=$[ $memTotalKb / 1024 ]
tmp=$[ $memTotalMb * 90 ]
xmxMb=$[ $tmp / 100 ]

# First try running java.
export JAVA_HOME=${d}/jdk1.7.0_40
echo JAVA_HOME is ${JAVA_HOME}
${JAVA_HOME}/bin/java -version

# Check that we can at least run H2O with the given java.
${JAVA_HOME}/bin/java -jar h2o.jar -version

# Start H2O disowned in the background.
nohup ${JAVA_HOME}/bin/java -Xmx${xmxMb}m -jar h2o.jar -name H2ODemo -flatfile flatfile.txt -port 54321 -beta -ice_root ${d}/ice_root 1> h2o.out 2> h2o.err &

