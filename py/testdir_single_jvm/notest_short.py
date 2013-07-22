import unittest
import random, sys, time, os
import string
import re
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_util
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        ### time.sleep(3600)
        h2o.tear_down_cloud()

    def test_short(self):
            csvFilename = 'short'
            key2 = csvFilename + ".hex"
            csvPathname = '/home/kevin/' + csvFilename

            # FIX! does 'separator=' take ints or ?? hex format
            # looks like it takes the hex string (two chars)
            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=30)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            ### inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename
            missingValuesDict = h2o_cmd.check_enums_from_inspect(parseKey)
            if missingValuesDict:
                m = [str(k) + ":" + str(v) for k,v in missingValuesDict.iteritems()]
                raise Exception("Looks like columns got flipped to NAs: " + ", ".join(m))

if __name__ == '__main__':
    h2o.unit_main()
