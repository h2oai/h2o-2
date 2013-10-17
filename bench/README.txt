This directory contains scripts to run benchmarks on the latest H2O build.

Use the `runBench.sh' script to perform the benchmarks:

USAGE: runBench.sh [options]

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

