#!/bin/bash
cd testdir_single_jvm
ls -1 test*py > n0
sed -i 's!^!./n0.doit !' n0

cd ../testdir_single_jvm_fvec
ls -1 test*py > n0
sed -i 's!^!./n0.doit !' n0

cd ../testdir_multi_jvm
ls -1 test*py > n0
sed -i 's!^!./n0.doit !' n0
sed -i 's!\(.*cloud.*\)!# \1!' ./n0

cd ../testdir_multi_jvm_fvec
ls -1 test*py > n0
sed -i 's!^!./n0.doit !' n0
sed -i 's!\(.*cloud.*\)!# \1!' ./n0

cd ../testdir_hosts
cp ../testdir_multi_jvm/n0 n1
sed -i 's!n0!n1!' ./n1
