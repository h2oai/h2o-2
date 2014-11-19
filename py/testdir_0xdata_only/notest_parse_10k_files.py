import unittest, time, sys, time, random, gzip, os
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i, h2o_glm, h2o_exec as h2e, h2o_jobs

def write_syn_dataset_gz(csvPathname, rowCount, headerData, rowData):
    f = gzip.open(csvPathname, 'wb')
    # Don't use the header..it's too small!
    # f.write(headerData + "\n")
    for i in range(rowCount):
        f.write(rowData + "\n")
    f.close()

def rand_rowData():
    # first column is output?
    rowData = str(random.randint(0,7))
    for i in range(100):
        rowData = rowData + "," + str(random.randint(0,7))
    return rowData

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()

    @classmethod
    def tearDownClass(cls):
        if not h2o.browse_disable:
            # time.sleep(500000)
            pass
        h2o.tear_down_cloud(h2o.nodes)
    
    def test_parse_10k_files(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        csvFilename = "syn.csv.gz"
        headerData = "ID,CAPSULE,AGE,RACE,DPROS,DCAPS,PSA,VOL,GLEASON"
        totalRows = 10
        maxFilenum = 10000
        for filenum in range(maxFilenum):
            rowData = rand_rowData()
            filePrefix = "%04d" % filenum
            csvPathname = SYNDATASETS_DIR + '/' + filePrefix + "_" + csvFilename
            write_syn_dataset_gz(csvPathname, totalRows, headerData, rowData)

        avgFileSize = os.path.getsize(csvPathname)

        importFolderPath = SYNDATASETS_DIR
        print "\nimportFolderPath:", importFolderPath
        csvFilenameList = [
            # try one thousand files first
            ("*[1][0-9][0-9][0-9]_syn.csv.gz", "syn_all.1000.csv", maxFilenum * avgFileSize, 1200),
            # try two thousand
            ("*[1-2][0-9][0-9][0-9]_syn.csv.gz", "syn_all.2000.csv", maxFilenum * avgFileSize, 1200),
            ]

        trialMax = 1
        tryHeap = 4
        DO_GLM = True
        noPoll = False
        benchmarkLogging = ['cpu','disk', 'iostats'] # , 'jstack'
        benchmarkLogging = ['cpu','disk']
        pollTimeoutSecs = 120
        retryDelaySecs = 10

        for i,(csvFilepattern, csvFilename, totalBytes, timeoutSecs) in enumerate(csvFilenameList):
            h2o.init(3,java_heap_GB=tryHeap, enable_benchmark_log=True)
            ### h2b.browseTheCloud()

            # don't let the config json redirect import folder to s3 or s3n, because
            # we're writing to the syn_datasets locally. (just have to worry about node 0's copy of this state)
            print "This test creates files in syn_datasets for import folder\n" + \
                "so h2o and python need to be same machine"
            h2o.nodes[0].redirect_import_folder_to_s3_path = False
            h2o.nodes[0].redirect_import_folder_to_s3n_path = False

            for trial in range(trialMax):
                # nice to have the list of what got imported, so we delete "just that" down below
                # doing this just so we can see what we import
                (importResult, importPattern) = h2i.import_only(path=importFolderPath+"/*")

                importFullList = importResult['files']
                print "importFullList:", importFullList
                importFailList = importResult['fails']
                print "importFailList:", importFailList
                print "\n Problem if this is not empty: importFailList:", h2o.dump_json(importFailList)

                h2o.cloudPerfH2O.change_logfile(csvFilename)
                h2o.cloudPerfH2O.message("")
                h2o.cloudPerfH2O.message("Parse " + csvFilename + " Start--------------------------------")
                start = time.time()
                parseResult = h2i.import_parse(path=importFolderPath+"/*",
                    hex_key=csvFilename + ".hex", timeoutSecs=timeoutSecs, 
                    retryDelaySecs=retryDelaySecs,
                    pollTimeoutSecs=pollTimeoutSecs,
                    noPoll=noPoll,
                    benchmarkLogging=benchmarkLogging)

                elapsed = time.time() - start
                print "Parse #", trial, "completed in", "%6.2f" % elapsed, "seconds.", \
                    "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

                if noPoll:
                    # does it take a little while to show up in Jobs, from where we issued the parse?
                    time.sleep(2)
                    # FIX! use the last (biggest?) timeoutSecs? maybe should increase since parallel
                    h2o_jobs.pollWaitJobs(pattern=csvFilename,
                        timeoutSecs=timeoutSecs, benchmarkLogging=benchmarkLogging)
                    totalBytes += totalBytes2 + totalBytes3
                    elapsed = time.time() - start
                    h2o.check_sandbox_for_errors()


                if totalBytes is not None:
                    fileMBS = (totalBytes/1e6)/elapsed
                    l = '{!s} jvms, {!s}GB heap, {:s} {:s} {:6.2f} MB/sec for {:.2f} secs'.format(
                        len(h2o.nodes), tryHeap, csvFilepattern, csvFilename, fileMBS, elapsed)
                    print l
                    h2o.cloudPerfH2O.message(l)

                print "Parse result['destination_key']:", parseResult['destination_key']

                # BUG here?
                if not noPoll:
                    h2o_cmd.get_columnInfoFromInspect(parseResult['destination_key'], exceptionOnMissingValues=True)
                        
                print "\n" + csvFilepattern

                #**********************************************************************************
                # Do GLM too
                # Argument case error: Value 0.0 is not between 12.0 and 9987.0 (inclusive)
                if DO_GLM:
                    GLMkwargs = {'y': 0, 'case': 1, 'case_mode': '>',
                        'max_iter': 10, 'n_folds': 1, 'alpha': 0.2, 'lambda': 1e-5}
                    start = time.time()
                    glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **GLMkwargs)
                    h2o_glm.simpleCheckGLM(self, glm, None, **GLMkwargs)
                    elapsed = time.time() - start
                    h2o.check_sandbox_for_errors()
                    l = '{:d} jvms, {:d}GB heap, {:s} {:s} GLM: {:6.2f} secs'.format(
                        len(h2o.nodes), tryHeap, csvFilepattern, csvFilename, elapsed)
                    print l
                    h2o.cloudPerfH2O.message(l)

                #**********************************************************************************

                h2o_cmd.checkKeyDistribution()
                h2i.delete_keys_from_import_result(pattern=csvFilename, importResult=importResult)

                h2o.tear_down_cloud()
                if not h2o.localhost:
                    print "Waiting 30 secs before building cloud again (sticky ports?)"
                    time.sleep(30)

                sys.stdout.write('.')
                sys.stdout.flush() 

if __name__ == '__main__':
    h2o.unit_main()
