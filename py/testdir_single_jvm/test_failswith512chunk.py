import unittest, time, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_fail1_100x1100(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='fail1_100x11000.csv.gz', schema='put',
            timeoutSecs=60, retryDelaySecs=0.15)

if __name__ == '__main__':
    h2o.unit_main()
