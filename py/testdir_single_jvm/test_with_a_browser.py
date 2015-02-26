import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o_cmd, h2o, h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # aws_credentials='~/.ec2/AwsCredentials.properties',
        # hdfs_config="~/.ec2/core-site.xml",
        # java_extra_args='-XX:+PrintGCDetails')
        # use_hdfs=True, 
        # Uses your username specific json: pytest_config-<username>.json
        h2o.init (1, java_heap_GB=28)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_with_a_browser(self):
        h2b.browseTheCloud()

        if not h2o.browse_disable:
            time.sleep(500000)

if __name__ == '__main__':

    h2o.unit_main()
