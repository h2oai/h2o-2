#!/bin/bash

set -e
echo "Gets the latest h2o.jar (only + version file) from s3, using curl"
echo "also copies the R/* downloaded stuff to target/R"

#**************************************
# do some bash parameters, just in case we have future expansion
# -n is no download of the jar
NO_DOWNLOAD=0
# default to hilbert
BRANCH=master
VERSION=
while getopts nb:v: flag
do
    case $flag in
        n)
            echo "Won't download the h2o.jar from S3. Copy from h2o-downloaded to target/h2o.jar and hadoop/target/*"
            NO_DOWNLOAD=1
            ;;
        b)
            BRANCH=$OPTARG
            echo "branch is $BRANCH"
            ;;
        v)
            VERSION=$OPTARG
            echo "version is $VERSION"
            ;;
        ?)
            exit
            ;;
    esac
done
shift $(( OPTIND - 1 ))  # shift past the last flag or argument
echo remaining parameters to Bash are $*

echo "using branch: $BRANCH"
echo "using version: $VERSION"

#**************************************
d=`dirname $0`
cd $d

rm -f ./latest_h2o_jar_version

curl -k --silent -o latest_h2o_jar_version https://h2o-release.s3.amazonaws.com/h2o/${BRANCH}/latest

if [[ $VERSION == "" || $VERSION == "latest" ]]
then
    version=$(<latest_h2o_jar_version)
else
    version=$VERSION
fi

echo "Using h2o jar version: $version"

curl -k --silent -o latest_h2o_project_version https://h2o-release.s3.amazonaws.com/h2o/${BRANCH}/${version}/project_version
project_version=$(<latest_h2o_project_version)

# a secret way to skip the download (use any arg)
if [ $NO_DOWNLOAD -eq 0 ]
then
    # echo "getting JUST $version/h2o.jar"
    rm -f ./h2o*$version.jar
    rm -f ./h2o*$version.zip
    zipurl=https://s3.amazonaws.com/h2o-release/h2o/${BRANCH}/${version}/h2o-${project_version}.zip
    echo Downloading ${zipurl} ...
    curl -k -o h2o-${project_version}.zip ${zipurl}
    mv h2o-*$version.zip h2o_$version.zip
fi

# this is the dir it unzips to
rm -f -r h2o*$version
unzip h2o_$version.zip

echo "copying h2o.jar to target/h2o.jar (overwrite)"
mkdir -p target
cp -f ./h2o*$version/h2o.jar target/h2o.jar
cp -f ./latest_h2o_jar_version target/latest_h2o_jar_version

echo "copying the downloaded R dir to  target/R"
echo "HACK. leave target/R contents in there from a make. need PACKAGES .."
# rm -f -r target/R
mkdir -p target/R
cp -f -r ./h2o*$version/R/* target/R

echo "copying the downloaded hadoop h2odriver*jar to target/hadoop"
mkdir -p target/hadoop
cp -f -r ./h2o*$version/hadoop/* target/hadoop

# copy here just to have one place where the last download files are
rm -f -r ./h2o-downloaded
cp -r ./h2o-*$version ./h2o-downloaded
echo "Done. Go forth and run tests."
echo "target/h2o-sources.jar is not touched, so it is out-of-step (but not used)"
echo "target/h2o-zookeeper.jar is not touched, so it is out-of-step (but not used)"
echo "target/h2o-model.jar is not touched, so it is out-of-step (but not used)"

# look at some stuff
cat ./latest_h2o_jar_version
ls -ltr ./latest_h2o_jar_version
ls -ltr ./h2o-downloaded/R/h2o*.tar.gz
ls -ltr ./h2o-*$version/hadoop/h2odriver*.jar
ls -ltr ./h2o-*$version/h2o.jar 
ls -ltr ./h2o-*$version/R/h2o*.tar.gz

