import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd

class Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        h2o.init()
        
    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        
    def test_cloud(self):
        h2o.touch_cloud()
        print "OK"

if __name__ == '__main__':
    h2o.unit_main()
