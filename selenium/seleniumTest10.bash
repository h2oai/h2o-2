#!/bin/bash

# ${WORKSPACE}/selenium/seleniumTest10.bash

# We use this script to test an HTML server.
# We are using WebDriver in Selenium.
# This runs an actual copy of Firefox inside of a vnc-display.

export J2REDIR=/usr/lib/jvm/java-6-oracle/jre
export J2SDKDIR=/usr/lib/jvm/java-6-oracle
export JAVA_HOME=/usr/lib/jvm/java-6-oracle
export LANG=en_US.UTF-8

export PATH=/usr/bin:/bin:${JAVA_HOME}/bin:${JAVA_HOME}/db/bin:${J2REDIR}/bin:/usr/local/bin

export myts=`/bin/date +%Y_%m_%d_%H_%M`
env|sort > /tmp/seleniumTest10_${myts}.bash.txt

# $WORKSPACE is usually set by jenkins.
if [ "$WORKSPACE" == "" ]
then
  echo jenkins sets  WORKSPACE to a dir.
  echo If you are not jenkins do this:
  echo export WORKSPACE=/tmp
  echo and run
  echo $0
  echo again
  exit 1
fi

pwd

mkdir -p ${WORKSPACE}/tmp/
cd       ${WORKSPACE}/tmp/

/bin/rm -f latest h2o-*.zip

# Get an integer which labels the latest build:
/usr/bin/s3cmd get s3://h2o-release/h2o/master/latest latest
/bin/cat latest
latest=`/bin/cat latest`

# debug
/usr/bin/s3cmd get "s3://h2o-release/h2o/master/${latest}/h2o-*${latest}.zip"
# latest=1037
# cp  ~dan/danhub/h2o_1037_bak.zip h2o-1.7.0.${latest}.zip
# debug

# Keep unzip happy, rm-rf the dir it wants to create:
/bin/rm -rf latest h2o-*${latest}

/usr/bin/unzip h2o-*${latest}.zip

# Prep to run h2o in background:
cd h2o-*${latest}
pwd

# H2OPORT is used in 2 places so I specify it here.
# I dont want to use the default value of 54321:
export H2OPORT=11185

# Kill old h2o maybe left behind. We want a new one.
ps waux|grep $H2OPORT|grep java|grep h2o.jar|/usr/bin/awk '{print $2}'|xargs /bin/kill

# Wait a bit:
sleep 3

# I want to run h2o in background:
if [ -f ${WORKSPACE}/selenium/run_h2o.bash ]
then
  echo I found:
  echo ${WORKSPACE}/selenium/run_h2o.bash
else
  echo ${WORKSPACE}/selenium/run_h2o.bash
  echo not found, will exit now.
  exit 1
fi

# Here:
${WORKSPACE}/selenium/run_h2o.bash > /tmp/run_h2o_bash_{$myts}.txt 2>&1 &

# Wait a bit:
echo 'Running in background now... wait for it...'
sleep 5

echo Here it is:
ps waux|grep $H2OPORT

echo Now I will set DISPLAY to vnc and run python-firefox-selenium test:
# debug
# Currently    localhost:1.0 should be tied to vnc server running on same host as jenkins.
export DISPLAY=localhost:1.0
# Dan desk:
# export DISPLAY=localhost:11.0
# debug

echo DISPLAY is:
echo $DISPLAY

cd      ${WORKSPACE}/selenium/
bash -x ${WORKSPACE}/selenium/javacTest24.bash
bash -x ${WORKSPACE}/selenium/javaTest24.bash

sleep 2

exit

