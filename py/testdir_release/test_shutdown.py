import unittest
sys.path.extend(['.','..','py'])
import h2o

print "Assumes you ran ../build_for_clone.py in this directory"
print "Creating a h2o-nodes.json to use. Also a sandbox dir!"
class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_shutdown(self):
        h2o.nodes.shutdown_all()

if __name__ == '__main__':
    h2o.unit_main()
