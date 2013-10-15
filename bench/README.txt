This directory contains scripts to run benchmarks on the latest H2O build.

Use the `runBench.sh' script to perform the benchmarks:

usage: runBench.sh [options]

This script obtains the latest h2o jar from S3 and runs the benchmarks for PCA, KMeans, GLM, and BigKMeans.

OPTIONS:
   -h      Show this message
   -t      Run task:
               Choices are:
                   ALL        -- Runs PCA, KMeans, GLM, and BigKMeans
                   PCA        -- Runs PCA on Airlines/AllBedrooms data
                   KMeans     -- Runs KMeans on Airlines/AllBedrooms data
                   GLM        -- Runs logistic regression on Airlines/AllBedrooms data
                   BigKMeans  -- Runs KMeans on 180 GB of synthetic data
   -j      JSON config:
               Choices are:
                   161        -- Runs benchmark(s) on single machine on 161
                   164        -- Runs benchmark(s) on single machine on 164
                   161_164    -- Runs benchmark(s) on four machines 161 - 164
