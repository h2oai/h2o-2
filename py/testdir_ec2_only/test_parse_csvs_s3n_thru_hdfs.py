
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
                # see https://github.com/0xdata/h2o/wiki/H2O-and-s3n
                hdfs_name_node='10.78.14.235:9000',
                hdfs_version='0.20.2')

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_all_s3n_thru_hdfs(self):
        print "\nLoad a list of files from s3n, parse it thru HDFS"
        print "In EC2, michal's config always passes the right config xml"
        print "as arg to the java -jar h2o.jar. Only works in EC2"

# h2o stdout should have this in ec2
# [h2o,hdfs] resource /tmp/3d50afd1a93d0da0092e5df73c44e679-core-site.xml added to the hadoop configuration

        timeoutSecs = 200
        ### imports3nResult = h2i.setupImports3n()
        # note s3n URI thru HDFS is not typical.
        URI = "s3n://home-0xdiag-datasets"
        importHDFSResult = h2o.nodes[0].import_hdfs(URI)
# "succeeded": [
#     {
#      "file": "billion_rows.csv.gz", 
#      "key": "s3n://home-0xdiag-datasets/billion_rows.csv.gz"
#    }, 
        s3nFullList = importHDFSResult['succeeded']
        print "s3nFullList:", h2o.dump_json(s3nFullList)
        # error if none? 
        self.assertGreater(len(s3nFullList),8,"Didn't see more than 8 files in s3n?")

        if (1==0):
            s3nList = random.sample(s3nFullList,8)
        else:
            s3nList = s3nFullList

        for s in s3nList:
            s3nKey = s['key']
            s3nFilename = s['file']
            # there is some non-file key names returned? s3n metadata?
            # only use the keys with csv in their name
            if ('csv' not in s3nKey) or ('syn_dataset' in s3nKey) or ('.gz' in s3nKey):
                continue

            # creates csvFilename.hex from file in hdfs dir 
            print "Loading s3n key: ", s3nKey, 'thru HDFS'
            parseKey = h2o.nodes[0].parse(s3nKey, s3nFilename + ".hex",
                timeoutSecs=500, retryDelaySecs=10, pollTimeoutSecs=60)

            print s3nFilename, 'parse time:', parseKey['response']['time']
            print "parse result:", parseKey['destination_key']

            start = time.time()
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
