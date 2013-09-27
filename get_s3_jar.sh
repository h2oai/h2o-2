#!/bin/bash
set -e

echo "You should be in your h2o dir running this."
echo ""
echo "Gets the latest h2o.jar (only + version file) from s3, using s3cmd"
echo ""
echo "s3cmd should have been installed by you: http://s3tools.org/download or http://s3tools.org/repositories"
echo "s3cmd --configure should have been run by you to set 0xdata aws key/id (once)." 
echo "If you need the keys, they might be in your ~/.ec2 dir: 'cat ~/.ec2/aws*' to see"
echo "This can run anywhere. No VPN needed, just s3cmd and keys"
echo ""
# set -v
# this can be master or a specific branch
branch=gauss

rm -f ./latest_h2o_jar_version
s3cmd get s3://h2o-release/h2o/$branch/latest latest_h2o_jar_version
version=$(<latest_h2o_jar_version)
echo "latest h2o jar version is: $version"
s3cmd ls s3://h2o-release/h2o/$branch/$version

# a secret way to skip the download (use any arg)
if [ $# -eq 0 ]
then
    # echo "getting JUST $version/h2o.jar"
    rm -f ./h2o*$version.jar
    rm -f ./h2o*$version.zip
    s3cmd ls s3://h2o-release/h2o/$branch/$version/
    s3cmd get s3://h2o-release/h2o/$branch/$version/h2o-*$version.zip 
    mv h2o-*$version.zip h2o_$version.zip
fi

# this is the dir it unzips to
rm -f -r h2o*$version
echo "Getting rid of any current h2oWrapper* files here"
unzip h2o_$version.zip

echo "moving h2o.jar and overwriting target/h2o.jar"
# jenkins might not have!
mkdir -p target
cp -f ./h2o*$version/h2o.jar target/h2o.jar
cp -f ./latest_h2o_jar_version target/latest_h2o_jar_version

# this is the one we point the R tests to. but we want a generic, no version name for them (like h2o/target)
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
ls -ltr ./h2o-*$version/R/h2oWrapper*.tar.gz
ls -ltr ./h2o-downloaded/R/h2oWrapper*.tar.gz



