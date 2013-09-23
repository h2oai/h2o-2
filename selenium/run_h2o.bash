#!/bin/bash

# ${WORKSPACE}/py/selenium/run_h2o.bash

# I use this script to run an h2o jar file.

# Assume parent set JAVA_HOME
export PATH=${JAVA_HOME}/bin:/usr/bin:/bin

# Hard-code the port and,
# assume I am in the right directory.
# The port needs to match the port I wrote into the Python test(s).
# Dont use this syntax, it causes my h2o-killer to fail:
## ${JAVA_HOME}/bin/java -Xmx50m -jar h2o.jar -port $H2OPORT -name seleniumJenkins
# My h2o-killer can kill this:
java -Xmx50m -jar h2o.jar -port $H2OPORT -name seleniumJenkins

exit
