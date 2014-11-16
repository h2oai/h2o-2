import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_browse as h2b, h2o_import as h2i
import time, random

print "This tries to mimic the use of the file pathname with ImportFile like R does"
print "Normally the python tests import the folder first, using h2o_import.import_parse()"
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(java_heap_GB=1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_import_file(self):
        timeoutSecs = 500
        cAll = [
            'smalldata/jira/v-3.csv',
            'smalldata/jira/v-3.csv',
            'smalldata/jira/v-3.csv',
            'smalldata/jira/v-3.csv',
            ]

        # pop open a browser on the cloud
        # h2b.browseTheCloud()

        for c in cAll:

            for i in range(10):
                # interesting. this will pass ../.. to h2o which is legal for h2o
                # but if this is run on a remote machine, we were assuming matching absolute paths
                # not relative to a current wd. I suppose we should test ../.. relative paths
                # but it would be local machine only (means you can't use this with -cj config.json ??
                csvPathname = h2o.find_file('smalldata/jira/v-3.csv')
                # http://172.16.2.222:54321/2/ImportFiles2.json?path=../../smalldata/jira/v-3.csv

                # race between remove and import?
                h2o.nodes[0].remove_all_keys()
                importResult = h2o.nodes[0].import_files(csvPathname, timeoutSecs=15)
                h2o.verboseprint(h2o.dump_json(importResult))
                files = importResult['files']
                keys = importResult['keys']
                fails = importResult['fails']
                dels = importResult['dels']

                if len(files) == 0:
                    raise Exception("empty files: %s after import" % files)
                if len(keys) == 0:
                    raise Exception("empty keys: %s after import" % keys)
                if len(fails) != 0:
                    raise Exception("non-empty fails: %s after import" % fails)
                if len(dels) != 0:
                    raise Exception("non-empty dels: %s after import" % dels)


if __name__ == '__main__':
    h2o.unit_main()
