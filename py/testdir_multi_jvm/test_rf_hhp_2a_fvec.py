import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_exec, h2o_rf

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_hhp_2a_fvec(self):
        csvFilenameList = {
            'hhp.cut3.214.data.gz',
            }

        for csvFilename in csvFilenameList:
            csvPathname = csvFilename
            print "RF start on ", csvPathname
            dataKeyTrain = 'rTrain.hex'
            start = time.time()
            parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=dataKeyTrain, schema='put',
                timeoutSecs=120)            
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            print "\n" + csvPathname, \
                "    numRows:", "{:,}".format(inspect['numRows']), \
                "    numCols:", "{:,}".format(inspect['numCols'])
            numCols = inspect['numCols']

            # we want the last col. Should be values 0 to 14. 14 most rare

            # from the cut3 set
            #   84777 0
            #   13392 1
            #    6546 2
            #    5716 3
            #    4210 4
            #    3168 5
            #    2009 6
            #    1744 7
            #    1287 8
            #    1150 9
            #    1133 10
            #     780 11
            #     806 12
            #     700 13
            #     345 14
            #    3488 15

            execExpr = "%s[,%s] = %s[,%s]==14" % (dataKeyTrain, numCols, dataKeyTrain, numCols)
            h2o_exec.exec_expr(None, execExpr, resultKey=dataKeyTrain, timeoutSecs=30)
            inspect = h2o_cmd.runInspect(key=dataKeyTrain)
            h2o_cmd.infoFromInspect(inspect, "going into RF")
            execResult = {'destination_key': dataKeyTrain}


            kwargs = {
                'ntrees': 2,
                'max_depth': 20,
                'nbins': 50,
            }
            rfView = h2o_cmd.runRF(parseResult=execResult, timeoutSecs=900, retryDelaySecs=300, **kwargs)
            print "RF end on ", csvPathname, 'took', time.time() - start, 'seconds'
            (error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView)


if __name__ == '__main__':
    h2o.unit_main()
