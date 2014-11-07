import unittest, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

# we have this rule for NUL. This test is not really following the  rule (afte eol)
# so will say it's not a valid test.
# Since NUL (0x00) characters may be used for padding, NULs are allowed, 
# and ignored after any line end, until the first non-NUL character. 
# NUL is not a end of line character, though. 
# verify: NUL (0x00) can be used as a character in tokens, and not be ignored. 


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_badchars(self):
        print "badchars.csv has some 0x0 (<NUL>) characters."
        print "They were created by a dd that filled out to buffer boundary with <NUL>"
        print "They are visible using vim/vi"
        
        csvPathname = 'badchars.csv'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=50, timeoutSecs=10)

if __name__ == '__main__':
    h2o.unit_main()
