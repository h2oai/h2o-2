This directory contains scripts to run benchmarks on the latest H2O build.

Use the `runBench.sh' script to perform the benchmarks:

USAGE: runBench.sh [options]

This script obtains the latest h2o jar from S3 and runs the benchmarks for PCA, KMeans, GLM, and BigKMeans.

OPTIONS:
   -h      Show this message
   -t      Run task:
               Choices are:
                   all        -- Runs PCA, KMeans, GLM, and BigKMeans
                   pca        -- Runs PCA on Airlines/AllBedrooms/Covtype data
                   kmeans     -- Runs KMeans on Airlines/AllBedrooms/Covtype data
                   glm        -- Runs logistic/linear regression on Airlines/AllBedrooms
                   glm2       -- Runs logistic/linear regression on Airlines/AllBedrooms
                   gbm        -- Runs GBM on Airlines/AllBedrooms/Covtype data
                   bigkmeans  -- Runs KMeans on 180 GB & 1TB of synthetic data

   -j      JSON config:
               Choices are:
                   161        -- Runs benchmark(s) on single machine on 161 (100GB)
                   162        -- Runs benchmark(s) on single machine on 162 (100GB)
                   163        -- Runs benchmark(s) on single machine on 163 (100GB)
                   164        -- Runs benchmark(s) on single machine on 164 (100GB)
                   161_163    -- Runs benchmark(s) on four machines 161-163 (150GB Each)
                   161_164    -- Runs benchmark(s) on four machines 161-164 (100GB Each)

   -b      Build number:
               Choices are limited to the available builds you have locally. Valid arguments are integral.

   -d      Debug mode:
               Run benchmark tasks in debug mode (smaller datasets).

   -L      Logging:
               Use this flag to enable logging on all machines passed in with the -j option.


Use the `startloggers.sh' script to capture logs. This script has other features and is used internally by `runBench.sh':

    A. Start big & little loggers (see BMLogs/README.txt)
    B. Stop loggers
    C. Clear caches on all systems passed in with the -j option to `runBench/sh'. Depends on root-owned compiled `flush.c' (w/ setuid bit set).
    D. Gather ICED logs
