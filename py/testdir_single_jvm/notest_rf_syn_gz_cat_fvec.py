import unittest, random, sys, time, math
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_exec as h2e, h2o_util

print "Create csv with lots of same data (98% 0?), so gz will have high compression ratio"
print "Cat a bunch of them together, to get an effective large blow up inside h2o"
print "Can also copy the files to test multi-file gz parse...that will behave differently"
print "Behavior may be different depending on whether small ints are used, reals or used, or enums are used"
print "Remember the enum translation table has to be passed around between nodes, and updated atomically"

print "response variable is the modulo sum of all features, for a given base"
print "\nThen do RF"

BASE = 2
DO_RF = False
NO_GZ = False
NO_REPL = False
DO_SUMMARY = False
DO_BLOCKING = 1
def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # 8 random generators, 1 per column
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        rowSum = 0
        for j in range(colCount):
            if BASE==2:
                # 50/50
                # r = h2o_util.choice_with_probability([(0, .5), (1, .5)])
                # 98/2
                r = h2o_util.choice_with_probability([(0, .98), (1, .2)])
            else:
                raise Exception("Unsupported BASE: " + BASE)

            rowSum += r


            rowData.append(r)

        responseVar = rowSum % BASE
        # make r a many-digit real, so gzip compresses even more better!
        rowData.append('%#034.32e' % responseVar)
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()

def make_datasetgz_and_parse(SYNDATASETS_DIR, csvFilename, hex_key, rowCount, colCount, FILEREPL, SEEDPERFILE, timeoutSecs):
    csvPathname = SYNDATASETS_DIR + '/' + csvFilename
    print "Creating random", csvPathname
    write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

    csvFilenamegz = csvFilename + ".gz"
    csvPathnamegz = SYNDATASETS_DIR + '/' + csvFilenamegz
    h2o_util.file_gzip(csvPathname, csvPathnamegz)

    csvFilenameReplgz = csvFilename + "_" + str(FILEREPL) + "x.gz"
    csvPathnameReplgz = SYNDATASETS_DIR + '/' + csvFilenameReplgz
    print "Replicating", csvFilenamegz, "into", csvFilenameReplgz

    start = time.time()
    h2o_util.file_cat(csvPathnamegz, csvPathnamegz , csvPathnameReplgz)
    # no header? should we add a header? would have to be a separate gz?
    totalRows = 2 * rowCount
    for i in range(FILEREPL-2):
        h2o_util.file_append(csvPathnamegz, csvPathnameReplgz)
        totalRows += rowCount
    print "Replication took:", time.time() - start, "seconds"

    start = time.time()
    print "Parse start:", csvPathnameReplgz

    # experiment to see if the gz is causing it to fail 
    if NO_GZ:
        csvPathnameReplgz = csvPathname
        totalRows = rowCount
    # hack experiment
    if NO_REPL:
        h2o_util.file_gzip(csvPathname, csvPathnameReplgz)
        totalRows = rowCount

    parseResult = h2i.import_parse(path=csvPathnameReplgz, schema='put', hex_key=hex_key, 
        timeoutSecs=timeoutSecs, pollTimeoutSecs=120, doSummary=DO_SUMMARY, blocking=DO_BLOCKING)

    if DO_SUMMARY:
        algo = "Parse and Summary:"
    else:
        algo = "Parse:"
    print algo , parseResult['destination_key'], "took", time.time() - start, "seconds"

    print "Inspecting.."
    time.sleep(5)
    start = time.time()
    inspect = h2o_cmd.runInspect(key=parseResult['destination_key'], timeoutSecs=timeoutSecs)
    print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
    h2o_cmd.infoFromInspect(inspect, csvPathnameReplgz)
    print "\n" + csvPathnameReplgz, \
        "    numRows:", "{:,}".format(inspect['numRows']), \
        "    numCols:", "{:,}".format(inspect['numCols'])

    # there is an extra response variable
    if inspect['numCols'] != (colCount + 1):
        raise Exception("parse created result with the wrong number of cols %s %s" % (inspect['numCols'], colCount))
    if inspect['numRows'] != totalRows:
        raise Exception("parse created result with the wrong number of rows (header shouldn't count) %s %s" % \
        (inspect['numRows'], totalRows))

    # hack it in! for test purposees only
    parseResult['numRows'] = inspect['numRows']
    parseResult['numCols'] = inspect['numCols']
    parseResult['byteSize'] = inspect['byteSize']
    return parseResult

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, tryHeap
        tryHeap = 4
        SEED = h2o.setup_random_seed()
        h2o.init(java_heap_GB=tryHeap, enable_benchmark_log=True)

    @classmethod
    def tearDownClass(cls):
        # time.sleep(3600)
        h2o.tear_down_cloud()

    def test_rf_syn_gz_cat(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        REPL = 3
        tryList = [
            # summary fails with 100000 cols
            # (10, 50, 2, 'cA', 600),
            # pass
            # (2, 50, 50, 'cA', 600),
            # (2, 100, 50, 'cA', 600),
            (REPL, 200, 50, 'cA', 600),
            (REPL, 300, 50, 'cA', 600),
            (REPL, 350, 50, 'cA', 600),
            (REPL, 375, 50, 'cB', 600),
            # fail
            (REPL, 500, 300, 'cC', 600),
            (REPL, 500, 400, 'cD', 600),
            (REPL, 500, 500, 'cE', 600),
            (10, 50, 1600, 'cF', 600),
            (10, 50, 3200, 'cG', 600),
            (10, 50, 5000, 'cH', 600),
            # at 6000, it gets connection reset on the parse on ec2
            # (6000, 50, 5000, 'cG', 600),
            # (7000, 50, 5000, 'cH', 600),
            ]

        ### h2b.browseTheCloud()

        paramDict = {
            'ntrees': 10,
            'destination_key': 'model_keyA',
            'max_depth': 10,
            'nbins': 100,
            'sample_rate': 0.80,
            }


        trial = 0
        for (FILEREPL, rowCount, colCount, hex_key, timeoutSecs) in tryList:
            trial += 1

            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            parseResult = make_datasetgz_and_parse(SYNDATASETS_DIR, csvFilename, hex_key, rowCount, colCount, FILEREPL, SEEDPERFILE, timeoutSecs)


            if DO_RF:
                paramDict['response'] = 'C' + str(colCount)
                paramDict['mtries'] = 2
                paramDict['seed'] = random.randint(0, sys.maxint)
                kwargs = paramDict.copy()

                start = time.time()
                rfView = h2o_cmd.runRF(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
                elapsed = time.time() - start
                print "RF end on ", parseResult['destination_key'], 'took', elapsed, 'seconds.', \
                    "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

                (classification_error, classErrorPctList, totalScores) = h2o_rf.simpleCheckRFView(rfv=rfView)

                algo = "RF " 
                l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs. trees: {:d} Error: {:6.2f} \
                    numRows: {:d} numCols: {:d} byteSize: {:d}'.format(
                    len(h2o.nodes), tryHeap, algo, parseResult['destination_key'], elapsed, kwargs['ntrees'], \
                    classification_error, parseResult['numRows'], parseResult['numCols'], parseResult['byteSize'])
                print l
                h2o.cloudPerfH2O.message(l)

            print "Trial #", trial, "completed"


if __name__ == '__main__':
    h2o.unit_main()
