#!/bin/sh

#
# This script is used by the REST API CollectLinuxInfo.
# See src/main/java/water/api/CollectLinuxInfo.java.
#
# The intent is to run this underneath the H2O log directory so that the info 
# gets collected automatically by the LogDownload task in LogView.
#
# We could be run on any variant of Linux, so some of the commands won't
# necessarily succeed, and there can be duplicates or logic to handle different
# Linux flavors.
#
# This script gets run as whatever use H2O is running as.  It is forked by Java.
# So it typically won't be root.  So many of the commands might fail, and we
# tolerate that.
#
# The script is invoked with /bin/sh.
#

if [ "$1" == "" ]; then
    echo First parameter must be directory to cd to
    exit 1
fi

# Exit on error.
set -e

cd $1

# Don't exit on error.
set +e

# General.
/bin/date > date.txt 2>&1

# Software.
/bin/cat /etc/redhat-release > redhat-release.txt 2>&1
/bin/cat /etc/lsb-release > lsb-release.txt 2>&1

# System kernel settings
/bin/cat /etc/sysctl.conf > sysctl.conf 2>&1

# Proc filesystem.
/bin/cat /proc/stat > proc_stat.txt 2>&1
/bin/cat /proc/cpuinfo > proc_cpuinfo.txt 2>&1
/bin/cat /proc/meminfo > proc_meminfo.txt 2>&1

# Processes.
/bin/ps -efww > ps_dash_efww.txt 2>&1
/bin/ps aux > ps_aux.txt 2>&1

# Network.
/sbin/ifconfig > ifconfig.txt 2>&1
/sbin/ifconfig -a > ifconfig_dash_a.txt 2>&1
/sbin/iptables --list > iptables_dash_list.txt 2>&1
/bin/netstat -s > netstat_dash_s.txt 2>&1
/bin/ss -s > socker.txt 2>&1
/bin/ip -s link > iplink.txt 2>&1
/usr/bin/nstat > nstat.txt 2>&1

# System.
/bin/uname -a > uname_dash_a.txt 2>&1
/sbin/sysctl -a > sysctl_dash_a.txt 2>&1
/usr/bin/vmstat 1 3 > vmstat_1_3.txt 2>&1
/usr/bin/top -b -d 1 -n 3 > top_dash_d1_n3.txt 2>&1
