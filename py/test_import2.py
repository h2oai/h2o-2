import unittest, time, sys, os
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..'])
import h2o_cmd
import h2o
import h2o_browse as h2b
import h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.build_cloud(node_count=1,java_heap_GB=1)
        h2b.browseTheCloud()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def notest_A_Basic(self):
        # put file and parse, starting from the current wd
        h2i.import_parse(path="testdir_multi_jvm/syn_sphere_gen.csv", schema='put')

    def notest_B_Basic(self):
        # put file and parse, will walk path looking upwards till it finds 'my-bucket' directory.
        # Getting the absolute path for mydata/file.csv starts there
        # default bucket name is 'home-0xdiag-datasets' (we can change that eventually)
        h2i.import_parse(path='dir2/syn_sphere_gen2.csv', bucket='my-bucket2', schema='put')
        
    def notest_C_Basic(self):
        # this will do an import folder and parse. schema='local' is default. doesn't need to be specified
        # I guess this will be relative to current wd

        ## if os env variable H2O_BUCKETS_ROOT is set, it will start looking there for bucket, then path
        ## that covers the case where "walking upward" is not sufficient for where you but the bucket (locally)
        os.environ['H2O_BUCKETS_ROOT'] = '/home'
        h2i.import_parse(path='dir3/syn_sphere_gen3.csv', bucket='my-bucket3', schema='local')
        del os.environ['H2O_BUCKETS_ROOT'] 
        
    def notest_D_Basic(self):
        # this can be an absolute path for the local system
        h2i.import_parse(path='/home/my-bucket2/dir2/syn_sphere_gen2.csv', schema='local')

    def test_E_Basic(self):
        # what happens here..abs path plus bucket. error?
        h2i.import_parse(path='/dir3/syn_sphere_gen3.csv', bucket='my-bucket3', schema='local')
        
    def test_F_Basic(self):
        # causes exception
        # h2i.import_parse(path="testdir_multi_jvm/syn_[1-2].csv", schema='put')

        # no exception
        h2i.import_parse(path="testdir_multi_jvm/syn[1-2].csv", schema='local')

        ## for specifying header_from_file...
        ## As long as header.csv was in the same directory (mydata), it will have been imported correctly.
        ## if not, another import_only step can be done (import itself does an import_only() step and a parse() step)

    def test_G_Basic(self):
        # defaults to import folder (schema='local')
        h2i.import_parse(path="testdir_multi_jvm/syn[1-2].csv")

    def test_H_Basic(self):
        # maybe best to extra the key from an import? first?
        # this isn't used much, maybe we don't care about this

        h2i.import_only(path="testdir_multi_jvm/syn_test/syn_header.csv")
        headerKey = h2i.find_key('syn_header.csv')
        # comma 44 is separator
        h2i.import_parse(path="testdir_multi_jvm/syn_test/syn[1-2].csv", header=1, header_from_file=headerKey, separator=44)
    
   
        # symbolic links work
        # ln -s /home/0xdiag/datasets home-0xdiag-datasets
        # lrwxrwxrwx 1 kevin kevin     21 Aug 26 22:05 home-0xdiag-datasets -> /home/0xdiag/datasets
        h2i.import_parse(path="standard/covtype.data", bucket="home-0xdiag-datasets")

        ## This will get it from import s3.
        #import(path=junkdir/junk.csv, bucket="home-0xdiag-datasets", schema="s3")
        #
        ## This will get it from import hdfs with s3n. the hdfs_name_node and hdfs_version for s3 
        # will have been passed at build_cloud, either from the test, or the <config>.json
        #import(path=junkdir/junk.csv, bucket="home-0xdiag-datasets", schema="s3n")
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
