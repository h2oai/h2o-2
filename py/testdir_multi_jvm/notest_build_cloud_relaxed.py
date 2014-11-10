import unittest, time, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..','../..','py'])
import h2o

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        print "Test cloud building with completion = one node says desired size plus consensus=1"
        print "Check is that all nodes agree on cloud size after completion rule"
        pass

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_build_cloud_relaxed_2(self):
        for trials in range(3):
            h2o.init(2,java_heap_GB=1, conservative=False)
            h2o.verify_cloud_size()
            h2o.tear_down_cloud()
            time.sleep(5)

    def test_B_build_cloud_relaxed_3(self):
        for trials in range(3):
            h2o.init(3,java_heap_GB=1, conservative=False)
            h2o.verify_cloud_size()
            h2o.tear_down_cloud()
            time.sleep(5)

    def test_C_build_cloud_relaxed_1(self):
        for trials in range(1):
            h2o.init(1,java_heap_GB=1, conservative=False)
            h2o.verify_cloud_size()
            h2o.tear_down_cloud()
            time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
