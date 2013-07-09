import os, json, unittest, time, shutil, sys, random
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts

def write_syn_dataset(csvPathname, rowCount, colCount, SEED):
    r1 = random.Random(SEED)
    dsf = open(csvPathname, "w+")

    for i in range(rowCount):
        rowData = []
        for j in range(colCount):
            ri = r1.randint(0,1)
            rowData.append(ri)

        ri = r1.randint(0,1)
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
        # SEED = 
        random.seed(SEED)

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
            (5000, 1, 'cD', 300),
            (5000, 2, 'cE', 300),
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
            legalValues = {0: 0, 1: 1}
            write_syn_dataset(csvPathname, rowCount, colCount, SEEDPERFILE)

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
                self.assertAlmostEqual(bins[0], .5 * rowCount, delta=.01*rowCount)
                self.assertAlmostEqual(bins[1], .5 * rowCount, delta=.01*rowCount)

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
                    self.assertEqual(smax[0], 1)
                    print "len(min):", len(smin), smin
                    self.assertEqual(smin[0], 0)

                    print "len(thresholds):", len(thresholds), thresholds
                    # FIX! what thresholds?

                    print "len(values):", len(values), values
                    for v in values:
                        self.assertIn(v,legalValues,"Value in percentile 'values' is not present in the dataset") 
                
                    print "mean:", mean
                    self.assertAlmostEqual(mean, 0.5, delta=0.01)
                    print "sigma:", sigma
                    self.assertAlmostEqual(sigma, 0.5, delta=0.01)

            ### print 'Trial:', trial
            sys.stdout.write('.')
            sys.stdout.flush()
            trial += 1


if __name__ == '__main__':
    h2o.unit_main()
