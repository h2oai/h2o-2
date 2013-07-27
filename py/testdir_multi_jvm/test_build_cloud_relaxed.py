import os, json, unittest, time, shutil, sys
# not needed, but in case you move it down to subdir
sys.path.extend(['.','..'])
import h2o_cmd
import h2o
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_A_build_cloud_relaxed_2(self):
        for trials in range(3):
            h2o.build_cloud(2,java_heap_GB=1, conservative=False)
            h2o.tear_down_cloud()

    def test_B_build_cloud_relaxed_3(self):
        for trials in range(3):
            h2o.build_cloud(3,java_heap_GB=1, conservative=False)
            h2o.tear_down_cloud()

    def test_C_build_cloud_relaxed_1(self):
        for trials in range(1):
            h2o.build_cloud(1,java_heap_GB=1, conservative=False)
            h2o.tear_down_cloud()

if __name__ == '__main__':
    h2o.unit_main()
