#!/bin/bash

#set -x

h2oBuild=
benchmarks="benchmarks"
DATE=`date +%Y-%m-%d`
archive="Archive"

function ALL() {
    echo "Running PCA benchmark..."
    PCA
    wait
    echo "Running KMeans benchmark..."
    KMeans
    wait
    echo "Running GLM benchmark..."
    GLM
    wait
    echo "Running GLM2..."
    GLM2
    wait
    echo "Running GBM..."
    GBM
    wait
    echo "Running GBMGrid..."
    GBMGrid
    wait
    echo "Running Big KMeans..."
    BigKMeans
    wait
}

function PCA() {
    pyScript="BMscripts/pcaBench.py"
    python ${pyScript} --config_json BMscripts/${JSON} ${h2oBuild}
    wait
    zip ${archive}/${h2oBuild}-${DATE}-PCA sandbox/
    wait
    rm -rf sandbox/
}

function KMeans() {
    pyScript="BMscripts/kmeansBench.py"
    python ${pyScript} --config_json BMscripts/${JSON} ${h2oBuild}
    wait
    zip ${archive}/${h2oBuild}-${DATE}-KMEANS sandbox/
    wait
    rm -rf sandbox/
}

function BigKMeans() {
    pyScript="BMscripts/bigkmeansBench.py"
    python ${pyScript} ${h2oBuild}
    wait
    zip ${archive}/${h2oBuild}-${DATE}-BIGKMEANS sandbox/
    wait
    rm sandbox/
}

function GLM() {
    pyScript="BMscripts/glmBench.py"
    python ${pyScript} --config_json BMscripts/${JSON} ${h2oBuild}
    wait
    zip ${archive}/${h2oBuild}-${DATE}-GLM sandbox/
    wait
    rm -rf sandbox/
}

function GLM2() {
    pyScript="BMscripts/glm2Bench.py"
    python ${pyScript} --config_json BMscripts/${JSON} ${h2oBuild}
    wait
    zip ${archive}/${h2oBuild}-${DATE}-GLM2 sandbox/
    wait
    rm -rf sandbox/
}


function GBM() {
    pyScript="BMscripts/gbmBench.py"
    python ${pyScript} --config_json BMscripts/${JSON} ${h2oBuild}
    wait
    zip ${archive}/${h2oBuild}-${DATE}-GBM sandbox/
    wait
    rm -rf sandbox/
}

function GBMGrid() {
    pyScript="BMscripts/gbmgridBench.py"
    python ${pyScript} --config_json BMscripts/${JSON} ${h2oBuild}
    wait
    zip ${archive}/${h2oBuild}-${DATE}-GBMGrid sandbox/
    wait
    rm -rf sandbox/
}


usage()
{
cat << EOF

USAGE: $0 [options]

This script obtains the latest h2o jar from S3 and runs the benchmarks for PCA, KMeans, GLM, and BigKMeans.

OPTIONS:
   -h      Show this message
   -t      Run task:
               Choices are:
                   ALL        -- Runs PCA, KMeans, GLM, and BigKMeans
                   PCA        -- Runs PCA on Airlines/AllBedrooms/Covtype data
                   KMeans     -- Runs KMeans on Airlines/AllBedrooms/Covtype data
                   GLM        -- Runs logistic regression on Airlines/AllBedrooms/Covtype data
                   GBM        -- Runs GBM on Airlines/AllBedrooms/Covtype data
                   BigKMeans  -- Runs KMeans on 180 GB & 1TB of synthetic data
                   
   -j      JSON config:
               Choices are:
                   161        -- Runs benchmark(s) on single machine on 161 (100GB)
                   162        -- Runs benchmark(s) on single machine on 162 (100GB)
                   163        -- Runs benchmark(s) on single machine on 163 (100GB)
                   164        -- Runs benchmark(s) on single machine on 164 (100GB)
 		   161_163    -- Runs benchmark(s) on four machines 161-163 (133GB Each)
                   161_164    -- Runs benchmark(s) on four machines 161-164 (100GB Each)
EOF
}

TASK=
JSON=
while getopts "ht:j:" OPTION
do
  case $OPTION in
    h)
      usage
      exit 1
      ;;
    t)
      TEST=$OPTARG
      ;;
    j)
      JSON=$OPTARG
      ;;
    ?)
      usage
      exit 1
      ;;
    *)
      usage
      exit 1
      ;;
  esac
done

if [ -z "$TEST" ] || [ -z "$JSON" ]
then
    usage
    exit
fi

#bash S3getLatest.sh
#wait
h2oBuild=`cat latest`

if [ ! -d ${benchmarks}/${h2oBuild}/${DATE} ]; then
  mkdir -p ${benchmarks}/${h2oBuild}/${DATE}
fi

if [ ! -d BMLogs/${h2oBuild}/${DATE} ]; then
  mkdir -p BMLogs/${h2oBuild}/${DATE}
fi

$TEST
wait

#remove annoying useless files
rm pytest*flatfile*
rm benchmark*log

#archive nohup
if [ -a nohup.out ]; then
    mv nohup.out ${archive}/${h2oBuild}-${DATE}-nohup.out
fi
