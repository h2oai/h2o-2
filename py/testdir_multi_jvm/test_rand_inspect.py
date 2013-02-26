import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_util

def ch1(n):
    return 0
def ch2(n):
    return n
def ch3(n):
    return n
def ch4(n):
    return int(n/2)
def ch5(n):
    return random.randint(0,n-1)
def good_choices(n):
    ch = h2o_util.choice_with_probability([(ch1,0.1), (ch2,0.1), (ch3,0.1), (ch4,0.1), (ch5,0.6)])
    return ch(n)

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = random.randint(0, sys.maxint)

        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()


    def test_rand_inspect(self):
        ### h2b.browseTheCloud()
        csvFilename = 'covtype.data'
        csvPathname = h2o.find_dataset('UCI/UCI-large/covtype/'+ csvFilename)
        print "\n" + csvPathname

        parseKey = h2o_cmd.parseFile(None, csvPathname, key=csvFilename, timeoutSecs=10)
        destination_key = parseKey['destination_key']
        print csvFilename, 'parse time:', parseKey['response']['time']
        print "Parse result['destination_key']:", destination_key 

        def inspect_and_check(nodeX,destination_key,offset,view,inspect=None):
            inspectNew = h2o_cmd.runInspect(h2o.nodes[nodeX], destination_key, offset=offset, view=view)
            # FIX! get min/max/mean/variance for a col too?
            constantNames = [
                'num_cols',
                'num_rows',
                ]
            if inspect is not None:
                for i in constantNames:
                    self.assertEqual(inspect[i], inspectNew[i])

            return inspectNew

        # going to use this to compare against future. num_rows/num_cols should always
        # be the same, regardless of the view. just a coarse sanity check
        origInspect = inspect_and_check(0,destination_key,0,1)
        h2o.verboseprint(h2o.dump_json(origInspect))

        num_rows = origInspect['num_rows']
        num_cols = origInspect['num_cols']

        lenNodes = len(h2o.nodes)
        for i in range (1000):
            # we want to use the boundary conditions, so have two level of random choices
            offset = good_choices(num_rows)
            view = good_choices(num_cols)
            # randomize the node used
            nodeX = random.randint(0,lenNodes-1)
            print "nodeX:", nodeX, "offset:", offset, "view:", view
            inspect_and_check(nodeX,destination_key,offset,view,origInspect)

            # do it again, once in a while
            r = random.randint(0,10)
            if (r==0):
                inspect_and_check(nodeX,destination_key,offset,view,origInspect)

if __name__ == '__main__':
    h2o.unit_main()
