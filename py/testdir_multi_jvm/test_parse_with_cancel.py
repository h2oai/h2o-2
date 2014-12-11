import unittest, time, sys, random
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_jobs

DELETE_KEYS = True
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init(3, java_heap_GB=4)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_parse_with_cancel(self):
        mustWait = 10
        importFolderPath = 'standard'
        timeoutSecs = 500
        csvFilenameList = [
            ("standard", "covtype.data", 54),
            ("manyfiles-nflx-gz", "file_1.dat.gz", 378),
            ("standard", "covtype20x.data", 54),
            ("manyfiles-nflx-gz", "file_[100-109].dat.gz", 378),
            ]

        # just loop on the same file. If remnants exist and are locked, we will blow up? 
        # Maybe try to do an inspect to see if either the source key or parse key exist and cause stack traces
        for (importFolderPath, csvFilename, response) in csvFilenameList:
            # creates csvFilename.hex from file in importFolder dir 
            csvPathname = importFolderPath + "/" + csvFilename 
            hex_key = csvFilename + ".hex"
            (importResult, importPattern) = h2i.import_only(bucket='home-0xdiag-datasets', path=csvPathname, timeoutSecs=50)

            start = time.time()
            parseResult = h2i.import_parse(bucket='home-0xdiag-datasets', path=csvPathname, hex_key=hex_key,
                timeoutSecs=500, noPoll=True, doSummary=False)
            job_key = parseResult['job_key']

            # give it a little time to start
            time.sleep(3)
            h2o.nodes[0].jobs_cancel(key=job_key)

            # now wait until the job cancels, and we're idle
            h2o_jobs.pollWaitJobs(timeoutSecs=30)
            elapsed = time.time() - start
            print "Cancelled parse completed in", elapsed, "seconds."

            h2o.check_sandbox_for_errors()
            # get a list of keys from storview. 20 is fine..shouldn't be many, since we putfile, not import folder
            # there maybe a lot since we import the whole "standard" folder
            # find the ones that pattern match the csvFilename, and inspect them. Might be none
            storeViewResult = h2o_cmd.runStoreView(timeoutSecs=timeoutSecs, view=100)
            keys = storeViewResult['keys']
            for k in keys:
                keyName = k['key']
                print "kevin:", keyName
                if csvFilename in keyName:
                    h2o_cmd.runInspect(key=keyName)
                    h2o.check_sandbox_for_errors()

            # This will tell h2o to delete using the key name from the import file, whatever pattern matches to csvFilename
            # we shouldn't have to do this..the import/parse should be able to overwrite without deleting.
            # h2i.delete_keys_from_import_result(pattern=csvFilename, importResult=importResult)

            # If you cancel a parse, you aren't allowed to reparse the same file or import a directory with that file,
            # or cause the key name that the parse would have used, for 5 seconds after the cancel request gets a json
            # response
            print "Waiting", mustWait, "seconds before next reparse-cancel."
            time.sleep(mustWait)

if __name__ == '__main__':
    h2o.unit_main()
