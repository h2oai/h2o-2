import unittest, sys
sys.path.extend(['.','..','py'])

# we have this rule for NUL. This test is not really following the  rule (afte eol)
# so will say it's not a valid test.
# Since NUL (0x00) characters may be used for padding, NULs are allowed, 
# and ignored after any line end, until the first non-NUL character. 
# NUL is not a end of line character, though. 
# verify: NUL (0x00) can be used as a character in tokens, and not be ignored. 

import h2o, h2o_cmd, h2o_hosts

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_badchars(self):
        print "badchars.csv has some 0x0 (<NUL>) characters."
        print "They were created by a dd that filled out to buffer boundary with <NUL>"
        print "They are visible using vim/vi"
        
        csvPathname = h2o.find_file('smalldata/badchars.csv')
        h2o_cmd.runRF(trees=50, timeoutSecs=10, csvPathname=csvPathname)

if __name__ == '__main__':
    h2o.unit_main()
