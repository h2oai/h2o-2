import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts, h2o_import as h2i, h2o_jobs

DO_POLL=True
DO_IGNORE=False
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

    def notest_B_kmeans_benign(self):
        h2o.beta_features = True # fvec
        importFolderPath = "logreg"
        csvFilename = "benign.csv"
        hex_key = "benign.hex"

        csvPathname = importFolderPath + "/" + csvFilename
        # FIX! hex_key isn't working with Parse2 ? parseResult['destination_key'] not right?
        print "\nStarting", csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, header=1, 
            timeoutSecs=180, noPoll=not DO_POLL, doSummary=False)

        if not DO_POLL:
            h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)
            parseResult['destination_key'] = hex_key
        
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\nStarting", csvFilename

        expected = [
            ([10.5, 2.8, 40.3, 0.0, 12.0, 0.8, 1.6, 21.1, 11.4, 0.7, 2.9, 206.2, 36.7, 1.5], 15, 0) ,
            ([23.72897196261682, 2.3271028037383177, 44.81308411214953, 0.34579439252336447, 13.093457943925234, 1.4579439252336448, 1.3177570093457944, 24.16129367150993, 13.317757009345794, 0.5071931108136043, 2.6604011393039024, 121.6822429906542, 40.13084112149533, 1.691588785046729], 110, 0) ,
            ([29.2625, 2.7, 48.5125, 0.1625, 12.0625, 1.0375, 1.4875, 23.023665714263917, 12.6875, 0.5073033705353737, 3.090870788693428, 160.95, 43.3, 1.65], 71, 0) ,
            ([38.333333333333336, 2.3333333333333335, 52.666666666666664, 0.0, 14.333333333333334, 2.3333333333333335, 1.6666666666666667, 25.85955047607422, 12.0, 0.5056179761886597, 3.2846442063649497, 261.6666666666667, 43.0, 1.0], 4, 0) ,
        ]

        # all are multipliers of expected tuple value
        allowedDelta = (0.01, 0.01, 0.01, 0.01)

        # loop, to see if we get same centers

        if DO_IGNORE:
            kwargs = {'k': 4, 'ignored_cols': 'STR', 'destination_key': 'benign_k.hex', 'seed': 265211114317615310, 'max_iter': 50}
        else:
            kwargs = {'k': 4, 'ignored_cols': None, 'destination_key': 'benign_k.hex', 'seed': 265211114317615310, 'max_iter': 50}

        kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=5, noPoll=not DO_POLL, **kwargs)

        if not DO_POLL:
            h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)
            # hack..supposed to be there like va
            kmeans['destination_key'] = 'benign_k.hex'
        ## h2o.verboseprint("kmeans result:", h2o.dump_json(kmeans))
        modelView = h2o.nodes[0].kmeans_model_view(model='benign_k.hex')
        h2o.verboseprint("KMeans2ModelView:", h2o.dump_json(modelView))
        model = modelView['model']
        clusters = model['clusters']
        within_cluster_variances = model['within_cluster_variances']
        total_within_SS = model['total_within_SS']
        print "within_cluster_variances:", within_cluster_variances
        print "total_within_SS:", total_within_SS

        # make this fvec legal?
        (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)
        h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=0)


    def test_C_kmeans_prostate(self):
        h2o.beta_features = True # fvec

        importFolderPath = "logreg"
        csvFilename = "prostate.csv"
        hex_key = "prostate.hex"
        csvPathname = importFolderPath + "/" + csvFilename
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, hex_key=hex_key, header=1, timeoutSecs=180)
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\nStarting", csvFilename

        # loop, to see if we get same centers
        expected = [
            ([43.07058823529412, 0.36470588235294116, 67.70588235294117, 1.1058823529411765, 2.3529411764705883, 1.2117647058823529, 17.33529411764706, 14.201176470588232, 6.588235294117647], 103, 0) ,
            ([166.04347826086956, 0.4658385093167702, 66.09316770186335, 1.0807453416149069, 2.3043478260869565, 1.0807453416149069, 15.0632298136646, 16.211118012422357, 6.527950310559007], 136, 0) ,
            ([313.4029850746269, 0.35074626865671643, 64.91791044776119, 1.0820895522388059, 2.1791044776119404, 1.0746268656716418, 14.601492537313437, 16.35686567164179, 6.082089552238806], 141, 0) ,
        ]

        # all are multipliers of expected tuple value
        allowedDelta = (0.01, 0.01, 0.01)
        kwargs = {'k': 3, 'initialization': 'Furthest', 'destination_key': 'prostate_k.hex', 'max_iter': 50,
            # reuse the same seed, to get deterministic results (otherwise sometimes fails
            'seed': 265211114317615310}

        # for fvec only?
        kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=5, noPoll=not DO_POLL, **kwargs)
        if not DO_POLL:
            h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)
            # hack..supposed to be there like va
            kmeans['destination_key'] = 'prostate_k.hex'
        # FIX! how do I get the kmeans result?
        ### print "kmeans result:", h2o.dump_json(kmeans)
        # can't do this
        # inspect = h2o_cmd.runInspect(key='prostate_k.hex')
        modelView = h2o.nodes[0].kmeans_model_view(model='prostate_k.hex')
        h2o.verboseprint("KMeans2ModelView:", h2o.dump_json(modelView))

        model = modelView['model']
        clusters = model['clusters']
        within_cluster_variances = model['within_cluster_variances']
        total_within_SS = model['total_within_SS']
        print "within_cluster_variances:", within_cluster_variances
        print "total_within_SS:", total_within_SS
        # variance of 0 might be legal with duplicated rows. wasn't able to remove the duplicate rows of NAs at 
        # bottom of benign.csv in ec2
        # for i,c in enumerate(within_cluster_variances):
        #    if c < 0.1:
        #        raise Exception("cluster_variance %s for cluster %s is too small. Doesn't make sense. Ladies and gentlemen, this is Chewbacca. Chewbacca is a Wookiee from the planet Kashyyyk. But Chewbacca lives on the planet Endor. Now think about it...that does not make sense!" % (c, i))
        

        # make this fvec legal?
        (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)
        h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=0)


if __name__ == '__main__':
    h2o.unit_main()
