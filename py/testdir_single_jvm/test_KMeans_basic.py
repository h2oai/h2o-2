import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_hosts

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

        kwargs = {'k': 1, 'epsilon': 1e-6, 'cols': None, 'destination_key': 'benign_k.hex'}
        kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, timeoutSecs=5, **kwargs)
        h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

    def test_C_kmeans_prostate(self):
        csvFilename = "prostate.csv"
        print "\nStarting", csvFilename
        csvPathname = h2o.find_file('smalldata/logreg' + '/' + csvFilename)
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=csvFilename + ".hex")

        kwargs = {'k': 1, 'epsilon': 1e-6, 'cols': None, 'destination_key': 'prostate_k.hex'}
        kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, timeoutSecs=5, **kwargs)
        h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
