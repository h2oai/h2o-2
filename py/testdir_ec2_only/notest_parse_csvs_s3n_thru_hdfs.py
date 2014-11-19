import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        h2o.init(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_all_s3n_thru_hdfs(self):
        print "\nLoad a list of files from s3n, parse it thru HDFS"
        print "In EC2, michal's config always passes the right config xml"
        print "as arg to the java -jar h2o.jar. Only works in EC2"

        bucket = 'home-0xdiag-datasets'
        csvPathname = 'standard/*'
        importResult = h2i.import_only(bucket=bucket, path=csvPathname, schema='s3n')
        s3nFullList = importResult['succeeded']
        print "s3nFullList:", h2o.dump_json(s3nFullList)
        self.assertGreater(len(s3nFullList),1,"Didn't see more than 1 files in s3n?")
        s3nList = random.sample(s3nFullList,8)

        timeoutSecs = 500
        for s in s3nList:
            s3nKey = s['key']
            s3nFilename = s['file']
            # there is some non-file key names returned? s3n metadata?
            # only use the keys with csv in their name
            if ('csv' not in s3nKey) or ('syn_dataset' in s3nKey) or ('.gz' in s3nKey):
                continue

            # creates csvFilename.hex from file in hdfs dir 
            print "Loading s3n key: ", s3nKey, 'thru HDFS'
            parseResult = h2i.parse_only(pattern=s3nKey, hex_key=s3nFilename + ".hex",
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=60)

            print "parse result:", parseResult['destination_key']

            start = time.time()
            sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
