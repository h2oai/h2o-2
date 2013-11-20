import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts, h2o_import as h2i, h2o_jobs

DO_POLL=True
DO_IGNORE=True # hits bug if true
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
        h2o.beta_features = True # fvec
        importFolderPath = "standard"
        csvFilename = "benign.csv"
        hex_key = "benign.hex"

        csvPathname = importFolderPath + "/" + csvFilename
        # FIX! hex_key isn't working with Parse2 ? parseResult['destination_key'] not right?
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key, header=1, 
            timeoutSecs=180, noPoll=not DO_POLL, doSummary=False)

        if not DO_POLL:
            h2o_jobs.pollWaitJobs(timeoutSecs=300, pollTimeoutSecs=300, retryDelaySecs=5)
            parseResult['destination_key'] = hex_key
        
        inspect = h2o_cmd.runInspect(None, parseResult['destination_key'])
        print "\nStarting", csvFilename

        expected = [
            ([23.10144927536232, 2.4927536231884058, 48.0, 0.21739130434782608, 12.565217391304348, 1.2028985507246377, 1.4057971014492754, 23.116674808663088, 12.826086956521738, 0.5451880801172447, 2.9851815665201102, 146.0144927536232, 42.84057971014493, 1.8985507246376812], 69, 32591.363626134153) ,
            ([25.68421052631579, 3.0526315789473686, 46.5, 0.02631578947368421, 12.236842105263158, 1.105263157894737, 1.5789473684210527, 22.387788290952102, 12.105263157894736, 0.5934358367829686, 2.9358367829686576, 184.5, 41.026315789473685, 1.5263157894736843], 38, 21419.904448700647) ,
            ([26.943181818181817, 2.272727272727273, 44.51136363636363, 0.38636363636363635, 12.840909090909092, 1.3636363636363635, 1.3181818181818181, 24.40187691521961, 13.477272727272727, 0.4736976506639427, 2.7090143003064355, 118.14772727272727, 40.13636363636363, 1.5568181818181819], 88, 44285.07981193549) ,
            ([31.8, 2.4, 48.2, 0.0, 13.4, 1.8, 1.6, 24.51573033707865, 11.8, 0.3033707865168539, 2.9707865168539325, 252.0, 41.4, 1.0], 5, 2818.6716828683248) ,
        ]

        # all are multipliers of expected tuple value
        allowedDelta = (0.01, 0.01, 0.01, 0.01)

        # loop, to see if we get same centers

        if DO_IGNORE:
            kwargs = {'k': 4, 'ignored_cols': 'STR', 'destination_key': 'benign_k.hex', 'seed': 265211114317615310}
        else:
            kwargs = {'k': 4, 'ignored_cols': None, 'destination_key': 'benign_k.hex', 'seed': 265211114317615310}

        # for fvec only?
        kwargs.update({'max_iter': 10})
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
        cluster_variances = model['cluster_variances']
        error = model['error']
        print "cluster_variances:", cluster_variances
        print "error:", error

        # make this fvec legal?
        (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)
        h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)


    def test_C_kmeans_prostate(self):
        h2o.beta_features = True # fvec

        importFolderPath = "standard"
        csvFilename = "prostate.csv"
        hex_key = "prostate.hex"
        csvPathname = importFolderPath + "/" + csvFilename
        parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key, header=1, timeoutSecs=180)
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
        for k in range(2, 6):
            kwargs = {'k': k, 'initialization': 'Furthest', 'destination_key': 'prostate_k.hex',
                # reuse the same seed, to get deterministic results (otherwise sometimes fails
                'seed': 265211114317615310}

            # for fvec only?
            kwargs.update({'max_iter': 50})

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
            cluster_variances = model['cluster_variances']
            error = model['error']
            print "cluster_variances:", cluster_variances
            print "error:", error
            # variance of 0 might be legal with duplicated rows. wasn't able to remove the duplicate rows of NAs at 
            # bottom of benign.csv in ec2
            # for i,c in enumerate(cluster_variances):
            #    if c < 0.1:
            #        raise Exception("cluster_variance %s for cluster %s is too small. Doesn't make sense. Ladies and gentlemen, this is Chewbacca. Chewbacca is a Wookiee from the planet Kashyyyk. But Chewbacca lives on the planet Endor. Now think about it...that does not make sense!" % (c, i))
            

            # make this fvec legal?
            (centers, tupleResultList) = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, trial=trial)



if __name__ == '__main__':
    h2o.unit_main()
