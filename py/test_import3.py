import unittest, time, sys, os
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..'])
import h2o_cmd
import h2o, h2o_hosts
import h2o_browse as h2b
import h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test3(self):
        # h2i.import_parse(path='standard/covtype.data', bucket='home-0xdiag-datasets', schema="s3n", timeoutSecs=60)
        ## This will get it from import hdfs with s3n. the hdfs_name_node and hdfs_version for s3 
        # will have been passed at build_cloud, either from the test, or the <config>.json
        h2i.import_parse(path='standard/benign.csv', bucket='home-0xdiag-datasets', schema='s3n', timeoutSecs=60)

        # h2i.import_parse(path='leads.csv', bucket='datasets', schema="hdfs", timeoutSecs=60)
        # h2i.import_parse(path='/datasets/leads.csv', schema="hdfs", timeoutSecs=60)
        # h2i.import_parse(path='datasets/leads.csv', schema="hdfs", timeoutSecs=60)

        ## This will get it from import s3.
        h2i.import_parse(path='standard/benign.csv', bucket='home-0xdiag-datasets', schema='s3', timeoutSecs=60)
        #import(path=junkdir/junk.csv, bucket="home-0xdiag-datasets", schema="s3")
        #
        ## this will get it from hdfs. the hdfs_name_node and hdfs_version for hdfs will 
        # have been passed at build_cloud, either from the test, or the <config>.json.
        ## It defaults to the local 172.16.2.176 cdh4 hdfs
        ## I guess -hdfs_root behavior works, but shouldn't be necessary (full path will be sent to h2o)
        #import(path=junkdir/junk.csv, bucket="home-0xdiag-datasets", schema="hdfs")
        #
        ## separator, exclude params can be passed for the parse
        #import(path=junkdir/junk.csv, bucket="home-0xdiag-datasets", schema="hdfs", separator=11)
        #
        #H2O_BUCKETS_ROOT is the only env variable that affects behavior
        #there are two <config.json> node variables set during build_cloud that will
        # redirect schema='local' to schema='s3n'
        # node.redirect_import_folder_to_s3_path
        # node.redirect_import_folder_to_s3n_path


if __name__ == '__main__':
    h2o.unit_main()
