import unittest, random, sys, time, math
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import as h2i, h2o_exec as h2e, h2o_util

print "Create csv with lots of same data (95% 0?), so gz will have high compression ratio"
print "Cat a bunch of them together, to get an effective large blow up inside h2o"
print "Can also copy the files to test multi-file gz parse...that will behave differently"
print "Behavior may be different depending on whether small ints are used, reals or used, or enums are used"
print "Remember the enum translation table has to be passed around between nodes, and updated atomically"

print "response variable is the modulo sum of all features, for a given base"
print "\nThen do RF"

BASE = 2
def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    # 8 random generators, 1 per column
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        rowSum = 0
        for j in range(colCount):
            if BASE==2:
                # we're just doing 50/50 for now, unlike the print says above
                r = h2o_util.choice_with_probability([(0, .5), (1, .5)])
            else:
                raise Exception("Unsupported BASE: " + BASE)

            rowSum += r
            rowData.append(r)

        responseVar = rowSum % BASE
        rowData.append(responseVar)
        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()

def make_datasetgz_and_parse(SYNDATASETS_DIR, csvFilename, key2, rowCount, colCount, FILEREPL, SEEDPERFILE, timeoutSecs):
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
    doSummary = False
    parseResult = h2o_cmd.parseFile(None, csvPathnameReplgz, key2=key2, timeoutSecs=timeoutSecs, pollTimeoutSecs=120, doSummary=doSummary)
    print csvFilenameReplgz, 'parse time:', parseResult['response']['time']
    if doSummary:
        algo = "Parse and Summary:"
    else:
        algo = "Parse:"
    print algo , parseResult['destination_key'], "took", time.time() - start, "seconds"

    print "Inspecting.."
    start = time.time()
    inspect = h2o_cmd.runInspect(None, parseResult['destination_key'], timeoutSecs=timeoutSecs)
    print "Inspect:", parseResult['destination_key'], "took", time.time() - start, "seconds"
    h2o_cmd.infoFromInspect(inspect, csvPathname)
    print "\n" + csvPathname, \
        "    num_rows:", "{:,}".format(inspect['num_rows']), \
        "    num_cols:", "{:,}".format(inspect['num_cols'])

    # there is an extra response variable
    if inspect['num_cols'] != (colCount + 1):
        raise Exception("parse created result with the wrong number of cols %s %s" % (inspect['num_cols'], colCount))
    if inspect['num_rows'] != totalRows:
        raise Exception("parse created result with the wrong number of rows (header shouldn't count) %s %s" % \
        (inspect['num_rows'], rowCount))

    # hack it in! for test purposees only
    parseResult['python_source_key'] = csvFilenameReplgz
    parseResult['num_rows'] = inspect['num_rows']
    parseResult['num_cols'] = inspect['num_cols']
    parseResult['value_size_bytes'] = inspect['value_size_bytes']
    return parseResult

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED, localhost, tryHeap
        tryHeap = 14
        SEED = h2o.setup_random_seed()
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1, java_heap_GB=tryHeap, enable_benchmark_log=True)
        else:
            h2o_hosts.build_cloud_with_hosts(enable_benchmark_log=True)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_syn_gz_cat(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            # summary fails with 100000 cols
            (10, 50, 5000, 'cA', 600),
            (50, 50, 5000, 'cB', 600),
            (100, 50, 5000, 'cC', 600),
            (500, 50, 5000, 'cD', 600),
            (1000, 50, 5000, 'cE', 600),
            (5000, 50, 5000, 'cF', 600),
            (6000, 50, 5000, 'cF', 600),
            (7000, 50, 5000, 'cF', 600),
            ]

        ### h2b.browseTheCloud()

        paramDict = {
            'class_weight': None,
            'ntree': 10,
            'model_key': 'model_keyA',
            'out_of_bag_error_estimate': 1,
            'stat_type': 'GINI',
            'depth': 2147483647,
            'bin_limit': 10000,
            'parallel': 1,
            'sample': 80,
            'exclusive_split_limit': 0,
            }


        trial = 0
        for (FILEREPL, rowCount, colCount, key2, timeoutSecs) in tryList:
            trial += 1

            SEEDPERFILE = random.randint(0, sys.maxint)
            csvFilename = 'syn_' + str(SEEDPERFILE) + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename
            parseResult = make_datasetgz_and_parse(SYNDATASETS_DIR, csvFilename, key2, rowCount, colCount, FILEREPL, SEEDPERFILE, timeoutSecs)

            paramDict['response_variable'] = colCount - 1
            paramDict['features'] = 9
            paramDict['seed'] = random.randint(0, sys.maxint)
            kwargs = paramDict.copy()

            start = time.time()
            rfView = h2o_cmd.runRFOnly(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "RF end on ", parseResult['python_source_key'], 'took', elapsed, 'seconds.', \
                "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            classification_error = rfView['confusion_matrix']['classification_error']
            ### self.assertLess(classification_error, 0.7, "Should have full classification error <0.7")

            algo = "RF " 
            l = '{:d} jvms, {:d}GB heap, {:s} {:s} {:6.2f} secs. trees: {:d} Error: {:6.2f} \
                num_rows: {:d} num_cols: {:d} value_size_bytes: {:d}'.format(
                len(h2o.nodes), tryHeap, algo, parseResult['python_source_key'], elapsed, kwargs['ntree'], \
                classification_error, parseResult['num_rows'], parseResult['num_cols'], parseResult['value_size_bytes'])
            print l
            h2o.cloudPerfH2O.message(l)

            print "Trial #", trial, "completed"


if __name__ == '__main__':
    h2o.unit_main()
