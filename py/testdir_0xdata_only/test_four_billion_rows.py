import unittest, time, sys, random
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_glm, h2o_browse as h2b, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1,java_heap_GB=14)
        else:
            h2o_hosts.build_cloud_with_hosts(base_port=54325, java_heap_GB=100)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_four_billion_rows(self):
        timeoutSecs = 1500

        importFolderPath = "billions"
        csvFilenameList = [
            ("four_billion_rows.csv", "a.hex"),
            ("four_billion_rows.csv", "b.hex"),
            ]
        for (csvFilename, hex_key) in csvFilenameList:
            csvPathname = importFolderPath + "/" + csvFilename
            start = time.time()

            # Parse*********************************
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, schema='local', hex_key=hex_key,
                timeoutSecs=timeoutSecs, pollTimeoutSecs=60)
            elapsed = time.time() - start
            print csvFilename, 'parse time:', parseResult['response']['time']
            print "Parse result['destination_key']:", parseResult['destination_key']
            print csvFilename, "completed in", elapsed, "seconds.", "%d pct. of timeout" % ((elapsed*100)/timeoutSecs)

            # Inspect*********************************
            # We should be able to see the parse result?
            inspect = h2o_cmd.runInspect(key=parseResult['destination_key'])
            num_cols = inspect['num_cols']
            num_rows = inspect['num_rows']
            value_size_bytes = inspect['value_size_bytes']
            row_size = inspect['row_size']
            print "\n" + csvFilename, \
                "    num_rows:", "{:,}".format(num_rows), \
                "    num_cols:", "{:,}".format(num_cols), \
                "    value_size_bytes:", "{:,}".format(value_size_bytes), \
                "    row_size:", "{:,}".format(row_size)

            expectedRowSize = num_cols * 1 # plus output
            expectedValueSize = expectedRowSize * num_rows
            self.assertEqual(row_size, expectedRowSize,
                msg='row_size %s is not expected num_cols * 1 byte: %s' % \
                (row_size, expectedRowSize))
            self.assertEqual(value_size_bytes, expectedValueSize,
                msg='value_size_bytes %s is not expected row_size * rows: %s' % \
                (value_size_bytes, expectedValueSize))

            summaryResult = h2o_cmd.runSummary(key=parseResult['destination_key'], timeoutSecs=timeoutSecs)
            h2o_cmd.infoFromSummary(summaryResult, noPrint=True)

            self.assertEqual(2, num_cols,
                msg="generated %s cols (including output).  parsed to %s cols" % (2, num_cols))
            self.assertEqual(4*1000000000, num_rows,
                msg="generated %s rows, parsed to %s rows" % (4*1000000000, num_rows))

            # KMeans*********************************
            kwargs = {
                'k': 3,
                'initialization': 'Furthest',
                'epsilon': 1e-6,
                'max_iter': 20,
                'cols': None,
                'normalize': 0,
                'destination_key': 'junk.hex',
                'seed': 265211114317615310,
                }

            timeoutSecs = 900
            start = time.time()
            kmeans = h2o_cmd.runKMeans(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)

            # GLM*********************************
            print "\n" + csvFilename
            kwargs = {'x': 0, 'y': 1, 'n_folds': 0, 'case_mode': '=', 'case': 1}
            # one coefficient is checked a little more
            colX = 0

            # L2 
            timeoutSecs = 900
            kwargs.update({'alpha': 0, 'lambda': 0})
            start = time.time()
            glm = h2o_cmd.runGLM(parseResult=parseResult, timeoutSecs=timeoutSecs, **kwargs)
            elapsed = time.time() - start
            print "glm (L2) end on ", csvFilename, 'took', elapsed, 'seconds.', "%d pct. of timeout" % ((elapsed/timeoutSecs) * 100)
            h2o_glm.simpleCheckGLM(self, glm, colX, **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
