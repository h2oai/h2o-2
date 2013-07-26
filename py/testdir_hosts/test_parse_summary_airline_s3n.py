
import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_hosts, h2o_glm
import h2o_browse as h2b
import h2o_import as h2i
import time, random

def infoFromSummary(self,summary):
    columnsList = summary['columns']
    for columns in columnsList:
        N = columns['N']
        # self.assertEqual(N, rowCount)
        name = columns['name']
        stype = columns['type']
        histogram = columns['histogram']
        bin_size = histogram['bin_size']
        bin_names = histogram['bin_names']
        for b in bin_names:
            print "bin_name:", b

        bins = histogram['bins']
        nbins = histogram['bins']
        print "\n\n************************"
        print "N:", N
        print "name:", name
        print "type:", stype
        print "bin_size:", bin_size
        print "len(bin_names):", len(bin_names), bin_names
        print "len(bins):", len(bins), bins
        print "len(nbins):", len(nbins), nbins

        # not done if enum
        if stype != "enum":
            smax = columns['max']
            smin = columns['min']
            mean = columns['mean']
            sigma = columns['sigma']
            print "smax:", smax
            print "smin:", smin
            print "mean:", mean
            print "sigma:", sigma

            # sometimes we don't get percentiles? (if 0 or 1 bins?)
            if len(bins) >= 2:
                percentiles = columns['percentiles']
                thresholds = percentiles['thresholds']
                values = percentiles['values']

                # h2o shows 5 of them, ordered
                print "len(max):", len(smax), smax
                print "len(min):", len(smin), smin
                print "len(thresholds):", len(thresholds), thresholds
                print "len(values):", len(values), values

                for v in values:
                    # 0 is the most max or most min
                    self.assertTrue(v >= smin[0],
                        "Percentile value %s should all be >= the min dataset value %s" % (v, smin[0]))
                    self.assertTrue(v <= smax[0],
                        "Percentile value %s should all be <= the max dataset value %s" % (v, smax[0]))

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # assume we're at 0xdata with it's hdfs namenode
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            # all hdfs info is done thru the hdfs_config michal's ec2 config sets up?
            h2o_hosts.build_cloud_with_hosts(1, 
                # this is for our amazon ec hdfs
                # see https://github.com/0xdata/h2o/wiki/H2O-and-s3n
                hdfs_name_node='10.78.14.235:9000',
                hdfs_version='0.20.2')

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_summary_airline_s3n(self):
        URI = "s3n://h2o-airlines-unpacked/"
        csvFilelist = [
            ("allyears2k.csv",   300), #4.4MB
            ("year1987.csv",     600), #130MB
            ("allyears.csv",     900), #12GB
            ("allyears_10.csv", 1800), #119.98GB
        ]
        # IMPORT**********************************************
        # since H2O deletes the source key, we should re-import every iteration if we re-use the src in the list
        importHDFSResult = h2o.nodes[0].import_hdfs(URI)
        ### print "importHDFSResult:", h2o.dump_json(importHDFSResult)
        s3nFullList = importHDFSResult['succeeded']
        ### print "s3nFullList:", h2o.dump_json(s3nFullList)

        self.assertGreater(len(s3nFullList),8,"Should see more than 8 files in s3n?")
        # why does this hang?
        if 1==0:
            storeView = h2o.nodes[0].store_view(timeoutSecs=60)
            for s in storeView['keys']:
                print "\nkey:", s['key']
                if 'rows' in s: 
                    print "rows:", s['rows'], "value_size_bytes:", s['value_size_bytes']

        trial = 0
        for (csvFilename, timeoutSecs) in csvFilelist:
            trialStart = time.time()
            csvPathname = csvFilename
            s3nKey = URI + csvPathname

            # PARSE****************************************
            key2 = csvFilename + "_" + str(trial) + ".hex"
            print "Loading s3n key: ", s3nKey, 'thru HDFS'
            start = time.time()
            parseKey = h2o.nodes[0].parse(s3nKey, key2,
                timeoutSecs=timeoutSecs, retryDelaySecs=10, pollTimeoutSecs=120)
            elapsed = time.time() - start
            print "parse end on ", s3nKey, 'took', elapsed, 'seconds',\
                "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)
            print "parse result:", parseKey['destination_key']

            # INSPECT******************************************
            # We should be able to see the parse result?
            start = time.time()
            inspect = h2o_cmd.runInspect(None, parseKey['destination_key'], timeoutSecs=360)
            print "Inspect:", parseKey['destination_key'], "took", time.time() - start, "seconds"
            h2o_cmd.infoFromInspect(inspect, csvPathname)
            # num_rows = inspect['num_rows']
            # num_cols = inspect['num_cols']

            (missingValuesDict, constantValuesDict, enumSizeDict, colTypeDict, colNameDict) = \
                h2o_cmd.columnInfoFromInspect(parseKey, timeoutSecs=300)

            # SUMMARY****************************************
            summaryResult = h2o.nodes[0].summary_page(key2, timeoutSecs=360)
            summary = summaryResult['summary']
            # print h2o.dump_json(summary)
            infoFromSummary(self, summary)

            # STOREVIEW***************************************
            if 1==0: # seems to timeout
                storeView = h2o.nodes[0].store_view()
                for s in storeView['keys']:
                    print "\nStoreView: key:", s['key']
                    if 'rows' in s: 
                        print "StoreView: rows:", s['rows'], "value_size_bytes:", s['value_size_bytes']

            print "Trial #", trial, "completed in", time.time() - trialStart, "seconds."
            trial += 0

if __name__ == '__main__':
    h2o.unit_main()
