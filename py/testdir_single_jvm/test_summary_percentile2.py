import os, json, unittest, time, shutil, sys, random
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts


def generate_scipy_comparison(csvPathname):
    # this is some hack code for reading the csv and doing some percentile stuff in scipy
    from numpy import loadtxt, genfromtxt, savetxt

    dataset = loadtxt(
        open(csvPathname, 'r'),
        delimiter=',',
        dtype='int16');

    print "csv read for training, done"

    # we're going to strip just the last column for percentile work
    # used below
    NUMCLASSES = 10
    print "csv read for training, done"

    # data is last column
    # drop the output
    print dataset.shape
    if 1==0:
        n_features = len(dataset[0]) - 1;
        print "n_features:", n_features

        # get the end
        target = [x[-1] for x in dataset]

        print "histogram of target"
        print sp.histogram(target,bins=NUMCLASSES)

        print target[0]
        print target[1]

    from scipy import stats
    stats.scoreatpercentile(dataset, [10,20,30,40,50,60,70,80,90])

def write_syn_dataset(csvPathname, rowCount, colCount, maxIntegerValue, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = r1.randint(0,maxIntegerValue)
            rowData.append(ri)

        rowDataCsv = ",".join(map(str,rowData))
        dsf.write(rowDataCsv + "\n")

    dsf.close()

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global SEED
        SEED = random.randint(0, sys.maxint)
        # if you have to force to redo a test
        # SEED = 
        random.seed(SEED)
        print "\nUsing random seed:", SEED

        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_summary(self):
        SYNDATASETS_DIR = h2o.make_syn_dir()
        tryList = [
            (500000, 1, 'cD', 300),
            (500000, 2, 'cE', 300),
        ]

        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        lenNodes = len(h2o.nodes)

        x = 0
        for (rowCount, colCount, key2, timeoutSecs) in tryList:
            SEEDPERFILE = random.randint(0, sys.maxint)
            x += 1

            csvFilename = 'syn_' + "binary" + "_" + str(rowCount) + 'x' + str(colCount) + '.csv'
            csvPathname = SYNDATASETS_DIR + '/' + csvFilename

            print "Creating random", csvPathname
            legalValues = {}
            maxIntegerValue = 9
            for x in range(maxIntegerValue+1):
                legalValues[x] = x
        
            expectedMin = min(legalValues)
            expectedMax = max(legalValues)
            expectedUnique = (expectedMax - expectedMin) + 1

            write_syn_dataset(csvPathname, rowCount, colCount, maxIntegerValue, SEEDPERFILE)

            parseKey = h2o_cmd.parseFile(None, csvPathname, key2=key2, timeoutSecs=10)
            print csvFilename, 'parse time:', parseKey['response']['time']
            print "Parse result['destination_key']:", parseKey['destination_key']

            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'])
            print "\n" + csvFilename


            summaryResult = n.summary_page(key2)
            # remove bin_names because it's too big (256?) and bins
            # just touch all the stuff returned
            summary = summaryResult['summary']
            print h2o.dump_json(summary)

            columnsList = summary['columns']
            for columns in columnsList:
                N = columns['N']
                self.assertEqual(N, rowCount)

                name = columns['name']

                stype = columns['type']
                self.assertEqual(stype, 'number')

                histogram = columns['histogram']
                bin_size = histogram['bin_size']
                self.assertEqual(bin_size, 1)

                bin_names = histogram['bin_names']
                bins = histogram['bins']
                # only values are 0 and 1
                for b in bins:
                    self.assertAlmostEqual(b, .1 * rowCount, delta=.01*rowCount)

                nbins = histogram['bins']

                print "\n\n************************"
                print "name:", name
                print "type:", stype
                print "N:", N
                print "bin_size:", bin_size
                print "len(bin_names):", len(bin_names), bin_names
                print "len(bins):", len(bins), bins
                print "len(nbins):", len(nbins), nbins

                # not done if enum
                if stype != "enum":
                    smax = columns['max']
                    smin = columns['min']
                    percentiles = columns['percentiles']
                    thresholds = percentiles['thresholds']
                    values = percentiles['values']
                    mean = columns['mean']
                    sigma = columns['sigma']

                    print "len(max):", len(smax), smax
                    self.assertEqual(smax[0], maxIntegerValue)
                    self.assertEqual(smax[1], maxIntegerValue-1)
                    self.assertEqual(smax[2], maxIntegerValue-2)
                    self.assertEqual(smax[3], maxIntegerValue-3)
                    self.assertEqual(smax[4], maxIntegerValue-4)
                    print "len(min):", len(smin), smin
                    self.assertEqual(smin[0], 0)
                    self.assertEqual(smin[1], 1)
                    self.assertEqual(smin[2], 2)
                    self.assertEqual(smin[3], 3)
                    self.assertEqual(smin[4], 4)

                    print "len(thresholds):", len(thresholds), thresholds
                    # FIX! what thresholds?

                    print "len(values):", len(values), values
                    # apparently our 'percentile estimate" uses interpolation, so this check is not met by h2o
                    for v in values:
                    ##    self.assertIn(v,legalValues,"Value in percentile 'values' is not present in the dataset") 
                    # but: you would think it should be within the min-max range?
                        self.assertTrue(v >= expectedMin, 
                            "Percentile value %s should all be >= the min dataset value %s" % (v, expectedMin))
                        self.assertTrue(v <= expectedMax, 
                            "Percentile value %s should all be <= the max dataset value %s" % (v, expectedMax))

                
                    print "mean:", mean
                    self.assertAlmostEqual(mean, maxIntegerValue/2.0, delta=0.1)
                    print "sigma:", sigma
                    # FIX! how do we estimate this
                    self.assertAlmostEqual(sigma, 2.9, delta=0.1)

            ### print 'Trial:', trial
            sys.stdout.write('.')
            sys.stdout.flush()
            trial += 1

            if (1==0): 
                generate_scipy_comparison(csvPathname)

if __name__ == '__main__':
    h2o.unit_main()

