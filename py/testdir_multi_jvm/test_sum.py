import unittest
import random, sys, time
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_util

# the shared exec expression creator and executor
import h2o_exec

zeroList = [
        'Result0 = 0',
]

# the first column should use this
exprList = [
        'Result<n> = sum(<keyX>[<col1>])',
    ]

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # random.seed(SEED)
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2)
        else:
            h2o_hosts.build_cloud_with_hosts()

        global SYNDATASETS_DIR
        SYNDATASETS_DIR = h2o.make_syn_dir()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_sum(self):
        print "Replicating covtype.data by 2x for results comparison to 1x"
        filename1x = 'covtype.data'
        pathname1x = h2o.find_dataset('UCI/UCI-large/covtype' + '/' + filename1x)
        filename2x = "covtype_2x.data"
        pathname2x = SYNDATASETS_DIR + '/' + filename2x
        h2o_util.file_cat(pathname1x,pathname1x,pathname2x)

        csvAll = [
            (pathname1x, "cA", 5,  1),
            (pathname2x, "cB", 5,  2),
            (pathname2x, "cC", 5,  2),
        ]

        h2b.browseTheCloud()
        lenNodes = len(h2o.nodes)

        firstDone = False
        for (csvPathname, key2, timeoutSecs, resultMult) in csvAll:
            parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=key2, timeoutSecs=2000)
            print "Parse result['Key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])

            print "\n" + csvPathname
            h2o_exec.exec_zero_list(zeroList)
            colResultList = h2o_exec.exec_expr_list_across_cols(lenNodes, exprList, key2, maxCol=54, 
                timeoutSecs=timeoutSecs)
            print "\ncolResultList", colResultList

            if not firstDone:
                colResultList0 = list(colResultList)
                good = [float(x) for x in colResultList0] 
                firstDone = True
            else:
                print "\n", colResultList0, "\n", colResultList
                # create the expected answer...i.e. N * first
                compare = [float(x)/resultMult for x in colResultList] 
                print "\n", good, "\n", compare
                self.assertEqual(good, compare, 'compare is not equal to good (first try * resultMult)')
        

if __name__ == '__main__':
    h2o.unit_main()
