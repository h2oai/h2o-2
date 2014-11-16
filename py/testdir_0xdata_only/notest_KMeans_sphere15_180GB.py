import unittest, time, sys, random, math, json
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_kmeans, h2o_import as h2i
import socket

# kevin@mr-0xb1:~/h2o/py/testdir_hosts$ ls -ltr /home3/0xdiag/datasets/kmeans_big
# -rw-rw-r-- 1 0xdiag 0xdiag 183538602156 Aug 24 11:43 syn_sphere15_2711545732row_6col_180GB_from_7x.csv
# -rwxrwxr-x 1 0xdiag 0xdiag         1947 Aug 24 12:21 sphere15_makeit
DO_KMEANS = True
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = h2o.setup_random_seed()
        h2o.init(enable_benchmark_log=True,
            use_hdfs=True, hdfs_version='cdh3', hdfs_name_node="mr-0x6") # override the config file

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_KMeans_sphere15_180GB(self):
        csvFilename = 'syn_sphere15_2711545732row_6col_180GB_from_7x.csv'
        totalBytes = 183538602156
        importFolderPath = "datasets/kmeans_big"
        csvPathname = importFolderPath + '/' + csvFilename

        # FIX! put right values in
        # will there be different expected for random vs the other inits?
        expected = [
            ([0.0, -113.00566692375459, -89.99595447985321, -455.9970643424373, 4732.0, 49791778.0, 36800.0], 248846122, 1308149283316.2988) ,
            ([0.0, 1.0, 1.0, -525.0093818313685, 2015.001629398412, 25654042.00592703, 28304.0], 276924291, 1800760152555.98) ,
            ([0.0, 5.0, 2.0, 340.0, 1817.995920197288, 33970406.992053084, 31319.99486705394], 235089554, 375419158808.3253) ,
            ([0.0, 10.0, -72.00113070337981, -171.0198611715457, 4430.00952228909, 37007399.0, 29894.0], 166180630, 525423632323.6474) ,
            ([0.0, 11.0, 3.0, 578.0043558141306, 1483.0163188052604, 22865824.99639042, 5335.0], 167234179, 1845362026223.1094) ,
            ([0.0, 12.0, 3.0, 168.0, -4066.995950679284, 41077063.00269915, -47537.998050740985], 195420925, 197941282992.43475) ,
            ([0.0, 19.00092954923767, -10.999565572612255, 90.00028669073289, 1928.0, 39967190.0, 27202.0], 214401768, 11868360232.658035) ,
            ([0.0, 20.0, 0.0, 141.0, -3263.0030236302937, 6163210.990273981, 30712.99115201907], 258853406, 598863991074.3276) ,
            ([0.0, 21.0, 114.01584574295777, 242.99690338815898, 1674.0029079209912, 33089556.0, 36415.0], 190979054, 1505088759456.314) ,
            ([0.0, 25.0, 1.0, 614.0032787274755, -2275.9931284021022, -48473733.04122273, 47343.0], 87794427, 1124697008162.3955) ,
            ([0.0, 39.0, 3.0, 470.0, -3337.9880599007597, 28768057.98852736, 16716.003410920028], 78226988, 1151439441529.0215) ,
            ([0.0, 40.0, 1.0, 145.0, 950.9990795199593, 14602680.991458317, -14930.007919032574], 167273589, 693036940951.0249) ,
            ([0.0, 42.0, 4.0, 479.0, -3678.0033024834297, 8209673.001421165, 11767.998552236539], 148426180, 35942838893.32379) ,
            ([0.0, 48.0, 4.0, 71.0, -951.0035145455234, 49882273.00063991, -23336.998167498707], 157533313, 88431531357.62982) ,
            ([0.0, 147.00394564757505, 122.98729664236723, 311.0047920137008, 2320.0, 46602185.0, 11212.0], 118361306, 1111537045743.7646) ,
        ]

        benchmarkLogging = ['cpu','disk', 'network', 'iostats', 'jstack']
        benchmarkLogging = ['cpu','disk', 'network', 'iostats']
        # IOStatus can hang?
        benchmarkLogging = ['cpu', 'disk', 'network']
        benchmarkLogging = []

        for trial in range(6):
            # IMPORT**********************************************
            # since H2O deletes the source key, re-import every iteration.
            # PARSE ****************************************
            print "Parse starting: " + csvFilename
            hex_key = csvFilename + "_" + str(trial) + ".hex"
            start = time.time()
            timeoutSecs = 2 * 3600
            kwargs = {}
            parseResult = h2i.import_parse(path=csvPathname, schema='hdfs', hex_key=hex_key,
                timeoutSecs=timeoutSecs, pollTimeoutSecs=60, retryDelaySecs=2,
                benchmarkLogging=benchmarkLogging, **kwargs)

            elapsed = time.time() - start
            fileMBS = (totalBytes/1e6)/elapsed
            l = '{!s} jvms, {!s}GB heap, {:s} {:s} {:6.2f} MB/sec for {:.2f} secs'.format(
                len(h2o.nodes), h2o.nodes[0].java_heap_GB, 'Parse', csvPathname, fileMBS, elapsed)
            print "\n"+l
            h2o.cloudPerfH2O.message(l)

            # KMeans ****************************************
            if not DO_KMEANS:
                continue

            print "col 0 is enum in " + csvFilename + " but KMeans should skip that automatically?? or no?"
            kwargs = {
                'k': 15, 
                'initialization': 'Furthest',
                'cols': None, 
                'destination_key': 'junk.hex', 
                # reuse the same seed, to get deterministic results
                'seed': 265211114317615310,
                }

            if (trial%3)==0:
                kwargs['initialization'] = 'PlusPlus'
            elif (trial%3)==1:
                kwargs['initialization'] = 'Furthest'
            else:
                kwargs['initialization'] = None

            timeoutSecs = 4 * 3600
            params = kwargs
            paramsString = json.dumps(params)

            start = time.time()
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs,
                    benchmarkLogging=benchmarkLogging, **kwargs)
            elapsed = time.time() - start
            print "kmeans end on ", csvPathname, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)

            l = '{!s} jvms, {!s}GB heap, {:s} {:s} {:s} for {:.2f} secs {:s}' .format(
                len(h2o.nodes), h2o.nodes[0].java_heap_GB, "KMeans", "trial "+str(trial), csvFilename, elapsed, paramsString)
            print l
            h2o.cloudPerfH2O.message(l)

            (centers, tupleResultList)  = h2o_kmeans.bigCheckResults(self, kmeans, csvPathname, parseResult, 'd', **kwargs)
            # all are multipliers of expected tuple value
            allowedDelta = (0.01, 0.01, 0.01) 
            h2o_kmeans.compareResultsToExpected(self, tupleResultList, expected, allowedDelta, allowError=True, trial=trial)

if __name__ == '__main__':
    h2o.unit_main()
