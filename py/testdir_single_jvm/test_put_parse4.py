import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_hosts, h2o_cmd

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1)
        else:
            h2o_hosts.build_cloud_with_hosts(node_count=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_put_parse4(self):
        timeoutSecs = 10
        trial = 1
        n = h2o.nodes[0]
        for x in xrange (2):
            # csvPathname = h2o.find_file("smalldata/hhp_107_01.data.gz")
            csvPathname = h2o.find_file('smalldata/iris/iris_wheader.csv.gz')
            key2 = "iris" + "_" + str(x) + ".hex"
            parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, key2=key2, doSummary=False)
            summaryResult = h2o_cmd.runSummary(key=key2)
            # remove bin_names because it's too big (256?) and bins
            # just touch all the stuff returned
            summary = summaryResult['summary']

            columnsList = summary['columns']
            for columns in columnsList:
                N = columns['N']
                name = columns['name']
                stype = columns['type']

                histogram = columns['histogram']
                bin_size = histogram['bin_size']
                bin_names = histogram['bin_names']
                bins = histogram['bins']
                nbins = histogram['bins']
                if 1==1:
                    print "\n\n************************"
                    print "name:", name
                    print "type:", stype
                    print "N:", N
                    print "bin_size:", bin_size
                    print "len(bin_names):", len(bin_names)
                    print "len(bins):", len(bins)
                    print "len(nbins):", len(nbins)

                # not done if enum
                if stype != "enum":
                    smax = columns['max']
                    smin = columns['min']
                    percentiles = columns['percentiles']
                    thresholds = percentiles['thresholds']
                    values = percentiles['values']
                    mean = columns['mean']
                    sigma = columns['sigma']
                    if 1==1:
                        print "len(max):", len(smax)
                        print "len(min):", len(smin)
                        print "len(thresholds):", len(thresholds)
                        print "len(values):", len(values)
                        print "mean:", mean
                        print "sigma:", sigma

            ### print 'Trial:', trial
            sys.stdout.write('.')
            sys.stdout.flush()
            trial += 1


if __name__ == '__main__':
    h2o.unit_main()
