import unittest, time, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..'])
import h2o_cmd
import h2o
import h2o_browse as h2b
import h2o_import2 as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1,java_heap_GB=1)

    @classmethod
    def tearDownClass(cls):
        time.sleep(3600)
        h2o.tear_down_cloud()

    def test_A_Basic(self):

        h2b.browseTheCloud()
        ## put file and parse, starting from the current wd
        h2i.import_parse(path="testdir_multi_jvm/syn_sphere_gen.csv", schema='put')

        #
        ## put file and parse, will walk path looking upwards till it finds 'my-bucket' directory.
        ## Getting the absolute path for mydata/file.csv starts there
        ## default bucket name is 'home-0xdiag-datasets' (we can change that eventually)
        #import(path=mydata/file.csv, bucket='my-bucket', schema='put')
        #
        ## this will do an import folder and parse. schema='local' is default. doesn't need to be specified
        ## I guess this will be relative to current wd
        #import(path=mydata/file.csv, schema='local')
        #
        ## this can be an absolute path for the local system
        #import(path=/mydata/file.csv, schema='local')
        #
        ## if os env variable H2O_BUCKETS_ROOT is set, it will start looking there for bucket, then path
        ## that covers the case where "walking upward" is not sufficient for where you but the bucket (locally)
        #import(path=mydata/file.csv, bucket='my-bucket')
        #
        ## same as above  except the import folder path stops one level above the pattern match,
        ## and parse will use the last part for the pattern match. It's all done with one parameter, rather than #
        ## separate import folder paths and regex pattern
        #import(path=mydata/file[0-5]*.csv)
        #
        ## for specifying header_from_file...
        ## As long as header.csv was in the same directory (mydata), it will have been imported correctly.
        ## if not, another import_only step can be done (import itself does an import_only() step and a parse() step)
        #import_only(path=mydata/header.csv)
        #import(path=mydata/file[0-5]*.csv, header=1, header_from_file=header.hex)
        #
        #
        ## separator, exclude params can be passed for the parse
        #import(path=mydata/file[0-5]*.csv, header=1, header_from_file=header.hex, separator=49)
        #
        #
        ## This will get it from import s3.
        #import(path=junkdir/junk.csv, bucket="home-0xdiag-datasets", schema="s3")
        #
        ## This will get it from import hdfs with s3n. the hdfs_name_node and hdfs_version for s3 will have been passed at build_cloud, either from the test, or the <config>.json
        #import(path=junkdir/junk.csv, bucket="home-0xdiag-datasets", schema="s3n")
        #
        ## this will get it from hdfs. the hdfs_name_node and hdfs_version for hdfs will have been passed at build_cloud, either from the test, or the <config>.json.
        ## It defaults to the local 192.168.1.176 cdh3 hdfs
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
