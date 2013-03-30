
import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts
import h2o_browse as h2b
import h2o_import as h2i
import time, random

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            # all hdfs info is done thru the hdfs_config michal's ec2 config sets up?
            h2o_hosts.build_cloud_with_hosts(1, 
            # this is for our amazon ec hdfs
            # see https://github.com/0xdata/h2o/wiki/H2O-and-S3
            hdfs_name_node='10.78.14.235:9000',
            hdfs_version='0.20.2')

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_all_S3_thru_hdfs(self):
        print "\nLoad a list of files from S3, parse it thru HDFS"
        print "In EC2, michal's config always passes the right config xml"
        print "as arg to the java -jar h2o.jar. Only works in EC2"

# h2o stdout should have this in ec2
# [h2o,hdfs] resource /tmp/3d50afd1a93d0da0092e5df73c44e679-core-site.xml added to the hadoop configuration

        timeoutSecs = 200
        ### importS3Result = h2i.setupImportS3()
        # note S3 URI thru HDFS is not typical.
        URI = "s3n://home-0xdiag-datasets"
        importHDFSResult = h2o.nodes[0].import_hdfs(URI)
# "succeeded": [
#     {
#      "file": "billion_rows.csv.gz", 
#      "key": "S3://home-0xdiag-datasets/billion_rows.csv.gz"
#    }, 
        S3FullList = importHDFSResult['succeeded']
        print "S3FullList:", h2o.dump_json(S3FullList)
        # error if none? 
        self.assertGreater(len(S3FullList,8),"Didn't see more than 8 files in S3?")

        if (1==0):
            S3List = random.sample(S3FullList,8)
        else:
            S3List = S3FullList

        for s in S3List:
            S3Key = S3List['key']
            S3Filename = S3List['file']

            # creates csvFilename.hex from file in hdfs dir 
            print "Loading S3 key: ", S3Key, 'thru HDFS'
            parseKey = h2o.nodes[0].parse(S3Key, S3Filename + ".hex",
                timeoutSecs=120, retryDelaySecs=5, pollTimeoutSecs=60)

            print S3Filename, 'parse time:', parseKey['response']['time']
            print "parse result:", parseKey['destination_key']

            print "\n" + S#Filename
            start = time.time()
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
