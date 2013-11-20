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

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_B_kmeans_benign(self):
        csvFilename = "benign.csv"
        print "\nStarting", csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path='logreg/'+csvFilename, schema='put', hex_key=csvFilename+".hex")

        expected = [
            ([23.10144927536232, 2.4927536231884058, 48.0, 0.21739130434782608, 12.565217391304348, 1.2028985507246377, 1.4057971014492754, 23.116674808663088, 12.826086956521738, 0.5451880801172447, 2.9851815665201102, 146.0144927536232, 42.84057971014493, 1.8985507246376812], 69, 32591.363626134153) ,
            ([25.68421052631579, 3.0526315789473686, 46.5, 0.02631578947368421, 12.236842105263158, 1.105263157894737, 1.5789473684210527, 22.387788290952102, 12.105263157894736, 0.5934358367829686, 2.9358367829686576, 184.5, 41.026315789473685, 1.5263157894736843], 38, 21419.904448700647) ,
            ([26.943181818181817, 2.272727272727273, 44.51136363636363, 0.38636363636363635, 12.840909090909092, 1.3636363636363635, 1.3181818181818181, 24.40187691521961, 13.477272727272727, 0.4736976506639427, 2.7090143003064355, 118.14772727272727, 40.13636363636363, 1.5568181818181819], 88, 44285.07981193549) ,
            ([31.8, 2.4, 48.2, 0.0, 13.4, 1.8, 1.6, 24.51573033707865, 11.8, 0.3033707865168539, 2.9707865168539325, 252.0, 41.4, 1.0], 5, 2818.6716828683248) ,
        ]
        # all are multipliers of expected tuple value
        allowedDelta = (0.01, 0.01, 0.01, 0.01)

        # loop, to see if we get same centers
        for trial in range(2):
            # 3 clusters wasn't stable? try 4 (3 wasn't stable in sklearn either)
            kwargs = {'k': 4, 'initialization': 'Furthest', 'cols': None, 'destination_key': 'benign_k.hex', 'max_iter': 20,
                # reuse the same seed, to get deterministic results (otherwise sometimes fails
                'seed': 265211114317615310}

            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=5, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvFilename, parseResult, 'd', **kwargs)
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)


    def test_C_kmeans_prostate(self):
        csvFilename = "prostate.csv"
        print "\nStarting", csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path='logreg/'+csvFilename, schema='put', hex_key=csvFilename+".hex")

        # loop, to see if we get same centers
        expected = [
            ([55.63235294117647], 68, 667.8088235294117) ,
            ([63.93984962406015], 133, 611.5187969924812) ,
            ([71.55307262569832], 179, 1474.2458100558654) ,
        ]

        # all are multipliers of expected tuple value
        allowedDelta = (0.01, 0.01, 0.01)
        for trial in range(2):
            kwargs = {'k': 3, 'initialization': 'Furthest', 'cols': 2, 'destination_key': 'prostate_k.hex', 'max_iter': 20,
                # reuse the same seed, to get deterministic results (otherwise sometimes fails
                'seed': 265211114317615310}

            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=5, **kwargs)
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvFilename, parseResult, 'd', **kwargs)

            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)



if __name__ == '__main__':
    h2o.unit_main()
