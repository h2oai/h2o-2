import unittest, random, sys, time, re
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i
# Result from exec is an interesting key because it changes shape depending on the operation
# it's hard to overwrite keys with other operations. so exec gives us that, which allows us
# to test key atomicity. (value plus size plus other aspects)
# Spin doing browser inspects while doing the key. play with whether python can multithread for
# us (python interpreter lock issues though)

# randomBitVector
# randomFilter
# log
# makeEnum
# bug?
#        'Result<n> = slice(<keyX>[<col1>],<row>)',
RAND_EXEC_NODE = False
exprList = [
        # to make sure all possible inspects are initted
        'Result1 = <keyX>[,<col1>]; Result2 = Result1; Result3 = Result1',
        'Result2 = <keyX>[,<col1>]',
        'Result4 = min(<keyX>[,<col1>])',
        'Result3 = <keyX>[,<col1>] + Result1',
        'Result4 = max(<keyX>[,<col1>]) + Result2',
        'Result3 = <keyX>[,<col1>] + Result1',
        'Result4 = mean(<keyX>[,<col1>]) + Result2',
        'Result3 = <keyX>[,<col1>] + Result1',
        'Result4 = sum(<keyX>[,<col1>]) + Result2',
        'Result3 = <keyX>[,<col1>] + Result1',
        'Result4 = min(<keyX>[,<col1>]) + Result2',
        'Result3 = <keyX>[,<col1>] + Result1',
        'Result4 = max(<keyX>[,<col1>]) + Result2',
        'Result3 = <keyX>[,<col1>] + Result1',
        'Result4 = mean(<keyX>[,<col1>]) + Result2',
        'Result3 = <keyX>[,<col1>] + Result1',
        'Result4 = sum(<keyX>[,<col1>]) + Result2',
    ]

inspectList = ['Result1', 'Result2', 'Result3']

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(3)


    @classmethod
    def tearDownClass(cls):
        # wait while I inspect things
        # time.sleep(1500)
        h2o.tear_down_cloud()

    def test_exec2_plus_browse(self):
        lenNodes = len(h2o.nodes)
        csvPathname = 'standard/covtype.data'
        hex_key = 'c.hex'
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='put', hex_key=hex_key, timeoutSecs=20)
        print "\nParse key is:", parseResult['destination_key']

        ## h2b.browseTheCloud()
        # for trial in range(53):
        trial = 0
        while (trial < 100):
            for exprTemplate in exprList:
                trial = trial + 1
                n = trial
                colX = random.randint(1,54)
                row = random.randint(1,400000)

                execExpr = exprTemplate
                execExpr = re.sub('<col1>',str(colX),execExpr)
                execExpr = re.sub('<col2>',str(colX+1),execExpr)
                execExpr = re.sub('<n>',str(n),execExpr)
                execExpr = re.sub('<row>',str(row),execExpr)
                execExpr = re.sub('<keyX>',str(hex_key),execExpr)

                # pick a random node to execute it on
                randNode = random.randint(0,lenNodes-1)
                print "\nexecExpr:", execExpr, "on node", randNode

                start = time.time()
                kwargs = {'str': execExpr}

                if RAND_EXEC_NODE:
                    resultExec = h2o_cmd.runExec(node=h2o.nodes[randNode], timeoutSecs=15, **kwargs)
                else:
                    resultExec = h2o_cmd.runExec(timeoutSecs=15, **kwargs)
                h2o.verboseprint(h2o.dump_json(resultExec))
                # print(h2o.dump_json(resultExec))

                # FIX! race conditions. If json is done, does that mean you can inspect it??
                # wait until the 2nd iteration, which will guarantee both Result1 and Result2 exist
                if trial > 1:
                    inspectMe = random.choice(inspectList)
                    resultInspect = h2o.nodes[0].inspect(inspectMe)
                    h2o.verboseprint(h2o.dump_json(resultInspect))

                    resultInspect = h2o.nodes[1].inspect(inspectMe)
                    h2o.verboseprint(h2o.dump_json(resultInspect))

                    resultInspect = h2o.nodes[2].inspect(inspectMe)
                    h2o.verboseprint(h2o.dump_json(resultInspect))

                # FIX! if we race the browser doing the exec too..it shouldn't be a problem?
                # might be a bug?

                # WARNING! we can't browse the Exec url history, since that will 
                # cause the Exec to execute again thru the browser..i.e. it has side effects
                # just look at the last inspect, which should be the resultInspect!
                h2b.browseJsonHistoryAsUrlLastMatch("Inspect")
                # h2b.browseJsonHistoryAsUrlLastMatch("Exec")
                h2o.check_sandbox_for_errors()
                print "exec end on ", "covtype.data" , 'took', time.time() - start, 'seconds'
                print "Trial #", trial, "completed\n"


if __name__ == '__main__':
    h2o.unit_main()
