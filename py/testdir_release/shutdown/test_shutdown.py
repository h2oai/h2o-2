import unittest, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_common

print "Assumes you ran ../build_for_clone.py in this directory"
print "Using h2o-nodes.json. Also the sandbox dir"
# uses a reduced common class
class releaseTest(h2o_common.ReleaseCommon2, unittest.TestCase):
    def test_shutdown(self):
        h2o.nodes[0].shutdown_all()

if __name__ == '__main__':
    h2o.unit_main()
