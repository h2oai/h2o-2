#!/bin/bash
#
# make_dmg
#
# make hfsplus disk image from directory
#
# sudo make_dmg.sh <dir_path> <dmg_name> [volume_label]
#
# http://confluence.concord.org/display/CCTR/Creating+MacOS+dmg+files+in+Linux
#

set -e
set -x

if [ -z "$SUDO_COMMAND" ]   # Need to run this with sudo 
then 
   mntusr=$(id -u) grpusr=$(id -g) sudo $0 $* 
   exit 0 
fi 

if [ -d "$1" ]              # dir_path 
then 
 dir_path=$1
else
  echo "Must pass in valid dir" 
  exit 
fi

if [ -n "$2" ]              # dmg_name
then 
 dmg_name=$2
else
  echo "Must pass name for dmg" 
  exit 
fi

if [ -n "$3" ]              # volume_label. 
then 
 volume_label=$3
else
  volume_label="Untitled"
  echo
  echo "Using volume_label=Untitled" 
  echo
fi

secs_since_epoch=`date "+%s"`
mnt_tmp=/mnt/tmp.$$.${secs_since_epoch}

du_output=`du -sk $dir_path 2>&1`
dir_size=`echo $du_output | cut -f1 -d" "`
dir_size=`expr $dir_size + 10240` 
dd if=/dev/zero of=$dmg_name bs=1024 count=$dir_size
/sbin/mkfs.hfsplus -v "$volume_label" $dmg_name
mkdir ${mnt_tmp}
mount -o loop -t hfsplus ./$dmg_name ${mnt_tmp}
cp -p -r $dir_path ${mnt_tmp}
umount ${mnt_tmp}
rmdir ${mnt_tmp}
