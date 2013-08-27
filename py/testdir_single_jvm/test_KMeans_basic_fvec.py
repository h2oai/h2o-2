import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)
        h2o.beta_features = True # fvec

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_kmeans_benign(self):
        importFolderPath = "/home/0xdiag/datasets/standard"
        csvFilename = "benign.csv"
        key2 = "benign.hex"
        csvPathname = importFolderPath + "/" + csvFilename
        h2i.setupImportFolder(None, importFolderPath)
        # FIX! key2 isn't working with Parse2 ? parseResult['destination_key'] not right?
        parseResult = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, key2=key2, header=1, timeoutSecs=180)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\nStarting", csvFilename

        expected = [
            ([24.538961038961038, 2.772727272727273, 46.89032467532467, 0.1266233766233766, 12.012142857142857, 1.0105194805194804, 1.5222727272727272, 22.26039690646432, 12.582467532467534, 0.5275062016635049, 2.9477601050634767, 162.52136363636365, 41.94558441558441, 1.661883116883117], 77, 46889.32010560476) ,
            ([25.587719298245613, 2.2719298245614037, 45.64035087719298, 0.35964912280701755, 13.026315789473685, 1.4298245614035088, 1.3070175438596492, 24.393307707470925, 13.333333333333334, 0.5244431302976542, 2.7326039818647745, 122.46491228070175, 40.973684210526315, 1.6754385964912282], 114, 64011.20272144667) ,
            ([30.833333333333332, 2.9166666666666665, 46.833333333333336, 0.0, 13.083333333333334, 1.4166666666666667, 1.5833333333333333, 24.298220973782772, 11.666666666666666, 0.37640449438202245, 3.404494382022472, 224.91666666666666, 39.75, 1.4166666666666667], 12, 13000.485226507595) ,

        ]
        # all are multipliers of expected tuple value
        allowedDelta = (0.01, 0.01, 0.01)

        # loop, to see if we get same centers
        for trial in range(2):
            kwargs = {'k': 3, 'epsilon': 1e-6, 'cols': None, 'destination_key': 'benign_k.hex',
                # reuse the same seed, to get deterministic results (otherwise sometimes fails
                'seed': 265211114317615310}

            # for fvec only?
            kwargs.update({'max_iter': 50, 'max_iter2': 1, 'iterations': 5})

            kmeans = h2o_cmd.runKMeansOnly(parseResult=parseKey, timeoutSecs=5, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)


    def test_C_kmeans_prostate(self):

        importFolderPath = "/home/0xdiag/datasets/standard"
        csvFilename = "prostate.csv"
        key2 = "prostate.hex"
        csvPathname = importFolderPath + "/" + csvFilename
        h2i.setupImportFolder(None, importFolderPath)
        parseResult = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, key2=key2, header=1, timeoutSecs=180)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\nStarting", csvFilename

        # loop, to see if we get same centers
        expected = [
            ([55.63235294117647], 68, 667.8088235294117) ,
            ([63.93984962406015], 133, 611.5187969924812) ,
            ([71.55307262569832], 179, 1474.2458100558654) ,
        ]

        # all are multipliers of expected tuple value
        allowedDelta = (0.01, 0.01, 0.01)
        for trial in range(2):
            kwargs = {'k': 3, 'initialization': 'Furthest', 'cols': 2, 'destination_key': 'prostate_k.hex',
                # reuse the same seed, to get deterministic results (otherwise sometimes fails
                'seed': 265211114317615310}

            # for fvec only?
            kwargs.update({'max_iter': 50, 'max_iter2': 1, 'iterations': 5})

            kmeans = h2o_cmd.runKMeansOnly(parseResult=parseKey, timeoutSecs=5, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)

            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)



if __name__ == '__main__':
    h2o.unit_main()
