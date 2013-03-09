import unittest
import random, sys, time, os
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_exec as h2e

# keep two lists the same size
# best if prime relative to the # jvms (len(h2o.nodes))
period = 11
initList = [
        'Result0 = -1',
        'Result1 = Result.hex + 1',
        'Result2 = Result.hex + 1',
        'Result3 = Result.hex + 1',
        'Result4 = 3',
        'Result5 = 4',
        'Result6 = 5',
        'Result7 = 6',
        'Result8 = 7',
        'Result9 = 8',
        'Result10 = 9',
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
            h2o.build_cloud(3,java_heap_GB=1)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec_assign(self):
        ### h2b.browseTheCloud()

        lenNodes = len(h2o.nodes)
        trial = 0
        while (trial < 200):
            for execExpr in initList:
                if (trial==100):
                    print "\nNow switching between nodes"

                if (trial < 100):
                    nodeX = 0
                else:
                    nodeX = random.randint(0,lenNodes-1)
                ### print nodeX

                resultKey = "Result" + str(trial % period)
                execResultInspect, min_value = h2e.exec_expr(h2o.nodes[nodeX], execExpr, 
                    resultKey=resultKey, timeoutSecs=4)

                ### print "\nexecResult:", execResultInspect

                print "trial: #" + str(trial), min_value, execExpr
                h2o.verboseprint("min_value: ", min_value, "trial:", trial)
                self.assertEqual(float(min_value), float((trial % period) - 1), 
                    "exec constant assigns don't seem to be getting done and visible to Inspect")

                sys.stdout.write('.')
                sys.stdout.flush()

                ### h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                trial += 1


if __name__ == '__main__':
    h2o.unit_main()
