
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
            h2o.build_cloud(1, use_hdfs=True, hdfs_version='cdh3', hdfs_name_node='192.168.1.176')
        else:
            h2o_hosts.build_cloud_with_hosts(1, use_hdfs=True, hdfs_version='cdh3', hdfs_name_node='192.168.1.176')

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_hdfs_multi_copies(self):
        print "\nUse the new regex capabilities for selecting hdfs: try *copies* at /datasets"
        print "This should match to a folder with about twenty covtype10x?"
        # pop open a browser on the cloud
        h2b.browseTheCloud()
        # defaults to /datasets
        h2i.setupImportHdfs()
        parseKey = h2o.nodes[0].parse('*covtype10x_copies*', key2='copies.hex', 
            exclude=None, header=None, timeoutSecs=600)
        print "*copies* regex to hdfs /datasets", 'parse time:', parseKey['response']['time']
        print "parse result:", parseKey['destination_key']
        sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
