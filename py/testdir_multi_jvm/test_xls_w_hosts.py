import unittest, time, sys
sys.path.extend(['.','..','py'])
import h2o, h2o_cmd, h2o_hosts, h2o_import2 as h2i

class TestExcel(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3,java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    # try a lot of trees
    def test_iris_xls(self):
        parseResult = h2i.import_parse(bucket='datasets', path='iris/iris.xls', schema='put')
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=100, timeoutSecs=5)

    def test_iris_xlsx(self):
        parseResult = h2i.import_parse(bucket='datasets', path='iris/iris.xlsx', schema='put')
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=100, timeoutSecs=5)

    def test_poker_xls(self):
        parseResult = h2i.import_parse(bucket='datasets', path='poker/poker-hand-testing.xls', schema='put')
        h2o_cmd.runRFOnly(parseResult=parseResult, trees=100, timeoutSecs=10)

    def test_poker_xlsx(self):
        parseResult = h2i.import_parse(bucket='datasets', path='poker/poker-hand-testing.xlsx', schema='put',
            timeoutSecs=120, pollTimeoutSecs=60)
        h2o_cmd.runRFOnly(None, parseResult=parseResult, trees=100, timeoutSecs=120)

if __name__ == '__main__':
    h2o.unit_main()
