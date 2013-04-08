import os, json, unittest, time, shutil, sys, random
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm, h2o_hosts, h2o_kmeans
import h2o_browse as h2b, h2o_import as h2i


class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(2,java_heap_GB=5)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_covtype20x(self):
        if localhost:
            csvFilenameList = [
                # 68 secs on my laptop?
                ('covtype20x.data', 480, 'cA'),
                ]
        else:
            # None is okay for key2
            csvFilenameList = [
                ('covtype20x.data', 480,'cA'),
                # ('covtype200x.data', 1000,'cE'),
                ]

        # a browser window too, just because we can
        h2b.browseTheCloud()

        importFolderPath = '/home/0xdiag/datasets'
        h2i.setupImportFolder(None, importFolderPath)
        for csvFilename, timeoutSecs, key2 in csvFilenameList:
            csvPathname = importFolderPath + "/" + csvFilename
            # creates csvFilename.hex from file in importFolder dir 
            start = time.time()
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, 
                timeoutSecs=2000, key2=key2, noise=('JStack', None))
            print "parse end on ", csvPathname, 'took', time.time() - start, 'seconds'
            h2o.check_sandbox_for_errors()

            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvPathname, \
                "    num_rows:", "{:,}".format(inspect['num_rows']), \
                "    num_cols:", "{:,}".format(inspect['num_cols'])

            kwargs = {
                'cols': None,
                'epsilon': 1e-4,
                'k': 2
            }

            start = time.time()
            kmeans = h2o_cmd.runKMeansOnly(parseKey=parseKey, \
                timeoutSecs=timeoutSecs, retryDelaySecs=2, pollTimeoutSecs=60, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_kmeans.simpleCheckKMeans(self, kmeans, **kwargs)

            ### print h2o.dump_json(kmeans)
            inspect = h2o_cmd.runInspect(None,key=kmeans['destination_key'])
            print h2o.dump_json(inspect)

if __name__ == '__main__':
    h2o.unit_main()
