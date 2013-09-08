import time
import h2o, h2o_import2 as h2i

class ReleaseCommon(object):
    def tearDown(self):
        print "tearDown"
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        print "setUpClass"
        # standard method for being able to reproduce the random.* seed
        h2o.setup_random_seed()
        h2o.build_cloud_with_json()

    @classmethod
    def tearDownClass(cls):
        print "tearDownClass"
        # DON"T
        ### h2o.tear_down_cloud()

        # try to download the logs...may fail again!
        h2o.nodes[0].log_download()

        # this could fail too
        if h2o.nodes[0].delete_keys_at_teardown:
            start = time.time()
            h2i.delete_keys_at_all_nodes()
            elapsed = time.time() - start
            print "delete_keys_at_all_nodes(): took", elapsed, "secs"

#***********************
# Notes:
# http://stackoverflow.com/questions/1323455/python-unit-test-with-base-and-sub-class
#     
# This method only works for setUp and tearDown methods if you reverse the order of the base classes. 
# Because the methods are defined in unittest.TestCase, and they don't call super(), 
# then any setUp and tearDown methods in CommonTests need to be first in the MRO, 
# or they won't be called at all. - Ian Clelland Oct 11 '10
#     
# If you add setUp and tearDown methods to CommonTests class, and you want them to be called for 
# each test in derived classes, you have to reverse the order of the base classes, 
# so that it will be: class SubTest1(CommonTests, unittest.TestCase). 
# - Denis Golomazov July 17
#***********************
