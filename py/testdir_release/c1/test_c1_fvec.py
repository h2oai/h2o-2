import unittest, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_common, h2o_print

print "Assumes you ran ../build_for_clone.py in this directory"
print "Using h2o-nodes.json. Also the sandbox dir"

print "For the 2nd test, which depends on nas access"
print "Uses 0xcust.. data. Cloud must be built as 0xcust.. and run with access to 0xdata /mnt automounts"
print "Assume there are links in /home/0xcust.. to the nas bucket used"
print "i.e. /home/0xcust... should have results of 'ln -s /mnt/0xcustomer-datasets"
print "The path resolver in python tests will find it in the home dir of the username being used"
print "to run h2o..i.e from the config json which builds the cloud and passes that info to the test"
print "via the cloned cloud mechanism (h2o-nodes.json)"

class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):

    def test_A_c1_fvec(self):
        parseResult = h2i.import_parse(bucket='smalldata', path='iris/iris2.csv', schema='put', timeoutSecs=60)
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=60)

    def test_B_c1_fvec(self):
        print "Since the python is not necessarily run as user=0xcust..., can't use a  schema='put' here"
        print "Want to be able to run python as jenkins"
        print "I guess for big 0xcust files, we don't need schema='put'"
        print "For files that we want to put (for testing put), we can get non-private files"
        parseResult = h2i.import_parse(bucket='0xcustomer-datasets', path='c1/iris2.csv', schema='local', timeoutSecs=60)
        h2o_cmd.runRF(parseResult=parseResult, trees=6, timeoutSecs=60)

if __name__ == '__main__':
    h2o.unit_main()
