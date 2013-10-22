Machine Logging Stats

Contents:
---------

This directory contains two files:

littleLogger.sh
bigLogger.sh
transpose.sh

Description:
------------

These scripts scrape cpu, memory, and network statistics 
from the following locations:

/proc/stat     -- Recovering user CPU, IDLE, and IOWAIT
/proc/meminfo  -- Recovering FREE, TOTAL, CACHED, and WRITEBACK
/proc/net/dev  -- Recovering received/transmitted BYTES, PACKETS, DROPS, ERRORS
top            -- Recovering the top 10 processes
vmstat         -- Recovering swap ins/outs

Deploy them with an ssh command to another machine to gather
performance statistics of that machine indefinitely.

Intended Usage:
---------------

The little logger is intended to be used for dumping
statistics by phase of the benchmark:
e.g. log file names are prepended by "PCA" or "KMEANS"
     or whatever is being benchmarked (passed in via
     argument) in order to denote the phase of the
     benchmark.

The big logger is intended to gather statistics
across the entire set of benchmark tasks.


Log Files Generated:
--------------------

bigLoggerPerf/
    {YYYY-MM-DD}-cpuPerf_machine.csv
    {YYYY-MM-DD}-idlePerf_machine.csv
    {YYYY-MM-DD}-iowaitPerf_machine.csv
    {YYYY-MM-DD}-memPerf_machine.csv
    {YYYY-MM-DD}-netRPerf_machine.csv
    {YYYY-MM-DD}-netTPerf_machine.csv
    {YYYY-MM-DD}-sisoPerf_machine.csv
    {YYYY-MM-DD}-topPerf_machine.csv

littleLoggerPerf/
    {PHASE}-{YYYY-MM-DD}-cpuPerf_machine.csv
    {PHASE}-{YYYY-MM-DD}-idlePerf_machine.csv
    {PHASE}-{YYYY-MM-DD}-iowaitPerf_machine.csv
    {PHASE}-{YYYY-MM-DD}-memPerf_machine.csv
    {PHASE}-{YYYY-MM-DD}-netRPerf_machine.csv
    {PHASE}-{YYYY-MM-DD}-netTPerf_machine.csv
    {PHASE}-{YYYY-MM-DD}-topPerf_machine.csv
    ...
