import unittest, time, sys, random, logging
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_import2 as h2i, h2o_glm, h2o_common
import h2o_print

print "Assumes you ran ../build_for_clone.py in this directory"
print "Creating a h2o-nodes.json to use. Also a sandbox dir!"
class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_A_c1_rel_short(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=10)

if __name__ == '__main__':
    h2o.unit_main()
