import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts
import h2o_browse as h2b
import h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, use_hdfs=True, hdfs_version='cdh3', hdfs_name_node='192.168.1.176')
        else:
            h2o_hosts.build_cloud_with_hosts(1, use_hdfs=True, hdfs_version='cdh3', hdfs_name_node='192.168.1.176')

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_hdfs_multi_bad_csv(self):
        print "\nUse the new regex capabilities for selecting hdfs: try *csv* at /datasets"
        # pop open a browser on the cloud
        h2b.browseTheCloud()
        # defaults to /datasets
        h2i.setupImportHdfs()
        # path should default to /datasets

# One .gz in with non .gz seems to cause a stack trace..so don't match to all (*airlines*).
# no..maybe it's just the zero length gz file?. No it doesn't show up in the list of keys?
# drwxr-xr-x   - earl   supergroup            0 2013-07-24 17:55 /datasets/airline.gz
# -rw-r--r--   3 hduser supergroup  12155501626 2013-02-22 17:13 /datasets/airline_116M.csv
# -rw-r--r--   3 hduser supergroup  11349125129 2013-05-03 15:45 /datasets/airlines_1988_2008.csv
# -rw-r--r--   3 hduser supergroup  11349125429 2013-05-01 12:52 /datasets/airlines_1988_2008_shuffled.csv
# -rw-r--r--   3 hduser supergroup         9936 2013-05-01 11:49 /datasets/airlines_88_08_100lines.csv
# -rw-r--r--   3 hduser supergroup  12155501626 2013-02-23 15:59 /datasets/airlines_all.csv
# -rw-r--r--   3 hduser supergroup 133710514626 2013-02-23 15:21 /datasets/airlines_all_11x.csv

        parseKey = h2i.parseImportHdfsFile(csvFilename='airline_116M.csv', key2='random_csv.hex', timeoutSecs=600)
        print "*csv* regex to hdfs /datasets", 'parse time:', parseKey['response']['time']
        print "parse result:", parseKey['destination_key']
        sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
