#!/bin/bash/

# Adjust based on the build of H2O you want to download.
h2oBranch=hilbert

echo "Fetching latest build number for branch ${h2oBranch}..."
curl -k --silent -o latest https://h2o-release.s3.amazonaws.com/h2o/${h2oBranch}/latest
h2oBuild=`cat latest`

echo "Fetching full version number for build ${h2oBuild}..."
curl -k --silent -o project_version https://h2o-release.s3.amazonaws.com/h2o/${h2oBranch}/${h2oBuild}/project_version
h2oVersion=`cat project_version`

curl --silent -o  h2o-${h2oVersion}.zip https://s3.amazonaws.com/h2o-release/h2o/${h2oBranch}/${h2oBuild}/h2o-${h2oVersion}.zip &
wait

unzip -o h2o-${h2oVersion}.zip 1> /dev/null &
wait

cp -f h2o-${h2oVersion}/h2o.jar ../target/h2o.jar &
wait

#rm latest
rm project_version
rm *zip
rm -rf h2o-${h2oVersion}
