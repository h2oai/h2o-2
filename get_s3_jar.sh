# echo/expand
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

rm -f ./latest_h2o_jar_version
s3cmd get s3://h2o-release/h2o/master/latest latest_h2o_jar_version
version=$(<latest_h2o_jar_version)
echo "latest h2o jar version is: $version"

s3cmd ls s3://h2o-release/h2o/master/$version

echo "getting JUST $version/h2o.jar"
rm -f ./h2o_$version.jar
s3cmd get s3://h2o-release/h2o/master/$version/h2o.jar h2o_$version.jar

echo "moving it and overwriting target/h2o.jar"
cp -f ./h2o_$version.jar target/h2o.jar
cp -f ./latest_h2o_jar_version target/latest_h2o_jar_version

echo ""
echo "Done. Go forth and run tests. If you build.sh or makefile, the h2o.jar will be overwritten"
echo "I left you a copy here though, if you don't want to download again. Just cp to target/h2o.jar"
echo "You may want to delete it if they accumulate"
echo "target/h2o-sources.jar is not touched, so it is out-of-step (but not used)"

ls -ltr ./latest_h2o_jar_version
cat ./latest_h2o_jar_version
ls -ltr ./h2o_$version.jar
ls -ltr ./target/h2o.jar


