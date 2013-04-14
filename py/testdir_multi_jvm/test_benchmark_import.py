import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd,h2o_hosts, h2o_browse as h2b, h2o_import as h2i, h2o_hosts
import h2o_exec as h2e
import time, random, logging

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_benchmark_import(self):
        # just do the import folder once
        # importFolderPath = "/home/hduser/hdfs_datasets"

        #    "covtype169x.data",
        #    "covtype.13x.shuffle.data",
        #    "3G_poker_shuffle"
        #    "covtype20x.data", 
        #    "billion_rows.csv.gz",

        # typical size of the michal files
        if (1==0):
            importFolderPath = '/home2/0xdiag/datasets'
            print "Using non-.gz'ed files in", importFolderPath
            avgMichalSize = 116561140 
            csvFilenameAll = [
                # I use different files to avoid OS caching effects
                ("manyfiles-nflx/file_1.dat", "file_1.dat", 1 * avgMichalSize, 700),
                ("manyfiles-nflx/file_[2][0-9].dat", "file_10.dat", 10 * avgMichalSize, 700),
                ("manyfiles-nflx/file_[34][0-9].dat", "file_20.dat", 20 * avgMichalSize, 700),
                ("manyfiles-nflx/file_[5-9][0-9].dat", "file_50.dat", 50 * avgMichalSize, 700),
                # ("manyfiles-nflx/file_[0-9][0-9]*.dat", "file_100.dat", 100 * avgMichalSize, 700),
                # ("onefile-nflx/file_1_to_100.dat", "file_single.dat", 100 * avgMichalSize, 1200),
            ]
        elif (1==0):
            importFolderPath = '/home2/0xdiag/datasets'
            print "Using non-.gz'ed files in", importFolderPath
            avgMichalSize = 116561140 
            csvFilenameAll = [
                # I use different files to avoid OS caching effects
                ("onefile-nflx/file_1_to_100.dat", "file_single.dat", 100 * avgMichalSize, 1200),
                ("manyfiles-nflx/file_[0-9][0-9]*.dat", "file_100.dat", 100 * avgMichalSize, 1200),
                ("manyfiles-nflx/file_1.dat", "file_1.dat", 1 * avgMichalSize, 700),
                ("manyfiles-nflx/file_[2][0-9].dat", "file_10.dat", 10 * avgMichalSize, 700),
                ("manyfiles-nflx/file_[34][0-9].dat", "file_20.dat", 20 * avgMichalSize, 700),
                ("manyfiles-nflx/file_[5-9][0-9].dat", "file_50.dat", 50 * avgMichalSize, 700),
            ]
        else:
            importFolderPath = '/home/0xdiag/datasets'
            print "Using .gz'ed files in", importFolderPath
            # all exactly the same prior to gzip!
            avgMichalSize = 237270000
            # could use this, but remember import folder -> import folder s3 for jenkins?
            # how would it get it right?
            # os.path.getsize(f)
            csvFilenameAll = [
                # ("manyfiles-nflx-gz/file_1[0-9].dat.gz", "file_10.dat.gz", 700),
                # 100 files takes too long on two machines?
                # ("covtype200x.data", "covtype200x.data", 15033863400, 700),
                # I use different files to avoid OS caching effects
                ("manyfiles-nflx-gz/file_1.dat.gz", "file_1.dat.gz", 1 * avgMichalSize, 700),
                # ("manyfiles-nflx-gz/file_10.dat.gz", "file_10_1.dat.gz", 1 * avgMichalSize, 700),
                # ("manyfiles-nflx-gz/file_1[0-9].dat.gz", "file_10.dat.gz", 10 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_[2][0-9].dat.gz", "file_10.dat.gz", 10 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_[34][0-9].dat.gz", "file_20.dat.gz", 20 * avgMichalSize, 700),
                ("manyfiles-nflx-gz/file_[5-9][0-9].dat.gz", "file_50.dat.gz", 50 * avgMichalSize, 700),
                # ("covtype200x.data", "covtype200x.data", 15033863400, 700),
                # ("manyfiles-nflx-gz/file_*.dat.gz", "file_100.dat.gz", 100 * avgMichalSize, 700),

                # do it twice
                # ("covtype.data", "covtype.data"),
                # ("covtype20x.data", "covtype20x.data"),
                # "covtype200x.data",
                # "100million_rows.csv",
                # "200million_rows.csv",
                # "a5m.csv",
                # "a10m.csv",
                # "a100m.csv",
                # "a200m.csv",
                # "a400m.csv",
                # "a600m.csv",
                # "billion_rows.csv.gz",
                # "new-poker-hand.full.311M.txt.gz",
                ]
        # csvFilenameList = random.sample(csvFilenameAll,1)
        csvFilenameList = csvFilenameAll


        # split out the pattern match and the filename used for the hex
        trialMax = 1
        # rebuild the cloud for each file
        base_port = 54321
        tryHeap = 28
        for (csvFilepattern, csvFilename, totalBytes, timeoutSecs) in csvFilenameList:
            localhost = h2o.decide_if_localhost()
            if (localhost):
                h2o.build_cloud(1,java_heap_GB=tryHeap, base_port=base_port,
                    enable_benchmark_log=True)
            else:
                h2o_hosts.build_cloud_with_hosts(1, java_heap_GB=tryHeap, base_port=base_port, 
                    enable_benchmark_log=True)
            # pop open a browser on the cloud
            ### h2b.browseTheCloud()

            # to avoid sticky ports?
            ### base_port += 2

            for trial in range(trialMax):
                importFolderResult = h2i.setupImportFolder(None, importFolderPath)
                importFullList = importFolderResult['succeeded']
                importFailList = importFolderResult['failed']
                print "\n Problem if this is not empty: importFailList:", h2o.dump_json(importFailList)
                # creates csvFilename.hex from file in importFolder dir 

                h2o.cloudPerfH2O.change_logfile(csvFilename)
                h2o.cloudPerfH2O.message("")
                h2o.cloudPerfH2O.message("Parse " + csvFilename + " Start--------------------------------")
                start = time.time()
                parseKey = h2i.parseImportFolderFile(None, csvFilepattern, importFolderPath, 
                    key2=csvFilename + ".hex", timeoutSecs=timeoutSecs, retryDelaySecs = 5, 
                    # benchmarkLogging=['cpu','disk', 'jstack'])
                    benchmarkLogging=['cpu','disk','jstack'])
                elapsed = time.time() - start
                print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
                if totalBytes is not None:
                    fileMBS = (totalBytes/1e6)/elapsed
                    print "\nMB/sec (before uncompress)", "%6.2f" % fileMBS
                    h2o.cloudPerfH2O.message('{!s} jvms, {!s}GB heap, {:s} {:s} {:6.2f} MB/sec for {:.2f} secs'.format(
                        len(h2o.nodes), tryHeap, csvFilepattern, csvFilename, fileMBS, elapsed))

                print csvFilepattern, 'parse time:', parseKey['response']['time']
                print "Parse result['destination_key']:", parseKey['destination_key']

                # We should be able to see the parse result?
                inspect = h2o_cmd.runInspect(key=parseKey['destination_key'])

                print "num_rows:", inspect['num_rows']
                print "num_cols:", inspect['num_cols']
                cols = inspect['cols']
                # trying to see how many enums we get
                # don't print int
                for i,c in enumerate(cols):
                    # print i, "name:", c['name']
                    msg = "column %d" % i
                    msg = msg + " type: %s" % c['type']
                    if c['type'] == 'enum':
                        msg = msg + (" enum_domain_size: %d" % c['enum_domain_size'])
                    if c['num_missing_values'] != 0:
                        msg = msg + (" num_missing_values: %s" % c['num_missing_values'])

                    if c['type'] != 'int' or (c['num_missing_values'] != 0):
                        print msg
                        
                # the nflx data doesn't have a small enough # of classes in any col
                # use exec to randomFilter out 200 rows for a quick RF. that should work for everyone?
                origKey = parseKey['destination_key']
                # execExpr = 'a = randomFilter('+origKey+',200,12345678)' 
                execExpr = 'a = slice('+origKey+',1,200)' 
                h2e.exec_expr(h2o.nodes[0], execExpr, "a", timeoutSecs=30)
                # runRFOnly takes the parseKey directly
                newParseKey = {'destination_key': 'a'}

                print "\n" + csvFilepattern
                start = time.time()
                # poker and the water.UDP.set3(UDP.java) fail issue..
                # constrain depth to 25
                print "Temporarily hacking to do nothing instead of RF on the parsed file"
                ### RFview = h2o_cmd.runRFOnly(trees=1,depth=25,parseKey=newParseKey, timeoutSecs=timeoutSecs)
                ### h2b.browseJsonHistoryAsUrlLastMatch("RFView")

                # remove the original data key
                for k in importFullList:
                    deleteKey = k['key']
                    ### print "possible delete:", deleteKey
                    # don't delete any ".hex" keys. the parse results above have .hex
                    # this is the name of the multi-file (it comes in as a single file?)
                    if csvFilename in deleteKey and not '.hex' in deleteKey:
                        print "\nRemoving", deleteKey
                        removeKeyResult = h2o.nodes[0].remove_key(key=deleteKey)
                        ### print "removeKeyResult:", h2o.dump_json(removeKeyResult)

                h2o.tear_down_cloud()
                if not localhost:
                    print "Waiting 30 secs before building cloud again (sticky ports?)"
                    time.sleep(30)

                sys.stdout.write('.')
                sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
