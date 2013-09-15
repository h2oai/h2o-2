import unittest, sys, os
sys.path.extend(['.','..','py'])
import h2o_cmd, h2o, h2o_hosts, h2o_import as h2i

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # default to local unless you've got hosts somewhere in the path to cwd
        local_host = h2o.decide_if_localhost()
        if (local_host):
            h2o.build_cloud(3,java_heap_GB=7)
        else:
            #  could have different config jsons, in different dirs (and execute out of those dirs)
            # Uses your username specific json: pytest_config-<username>.json
            # copy pytest_config-simple.json and modify to your needs.
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_simple_w_hosts(self):
        csvPathname = 'poker/poker1000'
        parseResult = h2i.import_parse(bucket='smalldata', path=csvPathname, schema='put')
        h2o_cmd.runRF(parseResult=parseResult, trees=50, timeoutSecs=60)

if __name__ == '__main__':
    h2o.unit_main()

