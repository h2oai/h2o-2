import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i, h2o_exec as h2e

print "Was getting a failure trying to write lock iris2_1.hex during the exec for iris2_2.hex"
print "Was it the GLM's locks on iris2_1.hex (prior) or the prior exec on iris2_1.hex. Dunno"
print "Focus on just fast back to back execs here..get rid of the glm"
print "maybe the putfile/parse? leave that in also"
print "maybe change to one upload, and don't delete the source file, so just reparse"

# avoid using delete_on_done = 0 because of h2o assertion error
AVOID_BUG = True
class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        h2o.init()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_exec2_fast_locks(self):
        csvPathname = 'iris/iris2.csv'
        src_key='iris.csv'
        if not AVOID_BUG:
            # need the key name (pattern) to feed to parse)
            (importResult, importPattern)  = h2i.import_only(bucket='smalldata', path=csvPathname, schema='put', 
                src_key=src_key, timeoutSecs=10)
            # just as a reminder of what these returns look like
            print "importResult:", h2o.dump_json(importResult)
            print "importPattern:", h2o.dump_json(importPattern)
        y = 4

        for trial in range (1, 100):
            if AVOID_BUG:
                # need the key name (pattern) to feed to parse)
                (importResult, importPattern)  = h2i.import_only(bucket='smalldata', path=csvPathname, schema='put', 
                    src_key=src_key, timeoutSecs=10)
                # just as a reminder of what these returns look like
                print "importResult:", h2o.dump_json(importResult)
                print "importPattern:", h2o.dump_json(importPattern)

            # make sure each parse is unique dest key (not in use)
            hex_key = "iris2_" + str(trial) + ".hex"
            # what if we kicked off another parse without waiting for it? I think the src key gets locked
            # so we'd get lock issues on the src_key
            parseResult = h2i.parse_only(pattern=src_key, hex_key=hex_key,
                delete_on_done=1 if AVOID_BUG else 0, timeoutSecs=10)
            execExpr="%s[,%s]=(%s[,%s]==%s)" % (hex_key, y+1, hex_key, y+1, 1)
            h2e.exec_expr(execExpr=execExpr, timeoutSecs=10)
            
        # just show the jobs still going, if any. maybe none, because short (iris)
        a = h2o.nodes[0].jobs_admin()
        h2o.verboseprint("jobs_admin():", h2o.dump_json(a))


if __name__ == '__main__':
    h2o.unit_main()

