#!/bin/bash

set -e

echo "Gets the latest h2o.jar (only + version file) from s3, using curl"

# this can be master or a specific branch
branch=master
# branch=gauss

d=`dirname $0`
cd $d

rm -f ./latest_h2o_jar_version

curl --silent -o latest_h2o_jar_version https://h2o-release.s3.amazonaws.com/h2o/${branch}/latest

version=$(<latest_h2o_jar_version)
echo "latest h2o jar version is: $version"

curl --silent -o latest_h2o_project_version https://h2o-release.s3.amazonaws.com/h2o/${branch}/${version}/project_version
project_version=$(<latest_h2o_project_version)

# a secret way to skip the download (use any arg)
if [ $# -eq 0 ]
then
    # echo "getting JUST $version/h2o.jar"
    rm -f ./h2o*$version.jar
    rm -f ./h2o*$version.zip
    zipurl=https://s3.amazonaws.com/h2o-release/h2o/${branch}/${version}/h2o-${project_version}.zip
    echo Downloading ${zipurl} ...
    curl -o h2o-${project_version}.zip ${zipurl}
    mv h2o-*$version.zip h2o_$version.zip
fi

# this is the dir it unzips to
rm -f -r h2o*$version
unzip h2o_$version.zip

echo "moving h2o.jar and overwriting target/h2o.jar"
mkdir -p target
cp -f ./h2o*$version/h2o.jar target/h2o.jar
cp -f ./latest_h2o_jar_version target/latest_h2o_jar_version

# this is the one we point the R tests to. but we want a generic, no version name for them (like h2o/target)
rm -f -r ./h2o-downloaded
cp -r ./h2o-*$version ./h2o-downloaded
echo "You want this for your R tests"
echo "export H2OWrapperDir=./h2o-downloaded/R"
echo ""
echo "Done. Go forth and run tests. If you build.sh or makefile, the h2o.jar will be overwritten"
echo "I left you a copy here though, if you don't want to download again. Just cp to target/h2o.jar"
echo "You may want to delete it if they accumulate"
echo "target/h2o-sources.jar is not touched, so it is out-of-step (but not used)"

# look at some stuff
ls -ltr ./latest_h2o_jar_version
cat ./latest_h2o_jar_version
ls -ltr ./h2o-*$version/h2o.jar 
ls -ltr target/h2o.jar
ls -ltr ./h2o-*$version/R/h2o*.tar.gz
ls -ltr ./h2o-downloaded/R/h2o*.tar.gz
ls -ltr ./h2o-downloaded/hadoop/h2odriver*.jar

