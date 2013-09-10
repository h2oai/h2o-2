#!/bin/bash
# 'common' test boilerplate looks for the cloud to be stable before starting
#  (which tests normally do if they build the cloud)
# 
# if the cloud dies, and restarts itself, the next test after a fail has a good 
# chance of running if it's timeout waiting for stabillize is long enough for the cloud restart
# 
# The ideal would be to copy the sandbox to another directory, for later analysis
# A limited # of retries would keep hell from breaking loss on continued failures.
# 
# Note the build is done in a test directory
# so ../ha_build.py should be used
for i in {0..10}
do
    ../build_for_clone.py
    cp -r sandbox sandbox_$i
done

# A new test would stall until the cloud responded (within timeout window) like it does today, waiting for the java to come up and respond.
# 
# 
