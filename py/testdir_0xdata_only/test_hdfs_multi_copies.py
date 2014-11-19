import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(1, use_hdfs=True, hdfs_version='cdh4', hdfs_name_node='mr-0x6')

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_hdfs_multi_copies(self):
        print "\nUse the new regex capabilities for selecting hdfs: try *copies* at /datasets"
        # pop open a browser on the cloud
        # h2b.browseTheCloud()
        # defaults to /datasets
        parseResult = h2i.import_parse(path='datasets/manyfiles-nflx-gz/*', schema='hdfs', hex_key='manyfiles.hex', 
            exclude=None, header=None, timeoutSecs=600)
        print "parse result:", parseResult['destination_key']
        sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
