import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd,h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_hosts
import time, random

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=10)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_import_multi_syn_datasets(self):
        # just do the import folder once
        # importFolderPath = "/home/hduser/hdfs_datasets"
        importFolderPath = '/home/0xdiag/datasets'
        h2i.setupImportFolder(None, importFolderPath)

        print "This imports a folder of csv files..i.e points to syn_datasets with no regex"
        print "Doesn't put anything in syn_datasets. When run with import folder redirected"
        print "to import S3, there is a syn_datasets with 100 files"
        print "FIX! When run locally, I should have some multi-files in", importFolderPath, "/syn_datasets?" 
        timeoutSecs = 500
        if h2o.nodes[0].redirect_import_folder_to_s3_path:
            csvFilenameAll = [
                # FIX! ..just folder doesn't appear to work. add regex
                # need a destination_key...h2o seems to use the regex if I don't provide one
                ### "syn_datasets/*", 
                "syn_datasets/*_10000x200*", 
                ]
        else:
            csvFilenameAll = [
                # FIX! ..just folder doesn't appear to work. add regex
                # need a destination_key...h2o seems to use the regex if I don't provide one
                ### "syn_datasets/*", 
                "syn_datasets/*", 
                ]

        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll

        # pop open a browser on the cloud
        ### h2b.browseTheCloud()

        for csvFilename in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            parseKey = h2i.parseImportFolderFile(None, csvFilename, importFolderPath, key2="syn_datasets.hex",
                timeoutSecs=500)
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            csvPathname = importFolderPath + "/" + csvFilename
            print "\n" + csvPathname, \
                "from all files num_rows:", "{:,}".format(inspect['num_rows']), \
                "num_cols:", "{:,}".format(inspect['num_cols'])

            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            kwargs = {'sample': 75, 'depth': 25, 'ntree': 1}
            start = time.time()
            RFview = h2o_cmd.runRFOnly(parseKey=parseKey, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            # so we can see!
            h2b.browseJsonHistoryAsUrlLastMatch("RFView")
            time.sleep(5)

if __name__ == '__main__':
    h2o.unit_main()
