import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts

def show_results(csvPathname, parseKey, model_key, centers, destination_key):
    kmeansApplyResult = h2o.nodes[0].kmeans_apply(
        data_key=parseKey['destination_key'], model_key=model_key, 
        destination_key=destination_key)
    # print h2o.dump_json(kmeansApplyResult)
    inspect = h2o_cmd.runInspect(None, destination_key)
    h2o_cmd.infoFromInspect(inspect, csvPathname)

    kmeansScoreResult = h2o.nodes[0].kmeans_score(
        key=parseKey['destination_key'], model_key=model_key)
    score = kmeansScoreResult['score']
    rows_per_cluster = score['rows_per_cluster']
    sqr_error_per_cluster = score['sqr_error_per_cluster']

    for i,c in enumerate(centers):
        print "\ncenters["+str(i)+"]: ", centers[i]
        print "rows_per_cluster["+str(i)+"]: ", rows_per_cluster[i]
        print "sqr_error_per_cluster["+str(i)+"]: ", sqr_error_per_cluster[i]

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
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")

        # loop, to see if we get same centers
        for i in range(2):
            kwargs = {'k': 3, 'epsilon': 1e-6, 'cols': None, 'destination_key': 'benign_k.hex'}
            kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, timeoutSecs=5, **kwargs)
            model_key = kmeans['destination_key']
            kmeansResult = h2o_cmd.runInspect(key=model_key)
            centers = kmeansResult['KMeansModel']['clusters']
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)
            show_results(csvPathname, parseKey, model_key, centers, 'd')


    def test_C_kmeans_prostate(self):
        csvFilename = "prostate.csv"
        print "\nStarting", csvFilename
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")

        # loop, to see if we get same centers
        for i in range(2):
            kwargs = {'k': 3, 'epsilon': 1e-6, 'cols': None, 'destination_key': 'prostate_k.hex'}
            kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, timeoutSecs=5, **kwargs)
            model_key = kmeans['destination_key']
            kmeansResult = h2o_cmd.runInspect(key=model_key)
            centers = kmeansResult['KMeansModel']['clusters']
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)
            show_results(csvPathname, parseKey, model_key, centers, 'd')

if __name__ == '__main__':
    h2o.unit_main()
