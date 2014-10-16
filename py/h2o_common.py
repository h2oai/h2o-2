import time, sys
import h2o, h2o_hosts, h2o_import as h2i

class SetupUnitTest(object):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            params = collectConf(cls)
            h2o.build_cloud(**params)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # if we got here by time out exception waiting for a job, we should clear
        # all jobs, if we're leaving h2o cloud up, and going to run another test
        #h2o.cancelAllJobs() 
        h2o.tear_down_cloud()


def collectConf(cls):
    result = { }
    if hasattr(cls, 'nodes'): result['node_count'] = cls.nodes
    if hasattr(cls, 'java_xmx'): result['java_heap_GB'] = cls.java_xmx
    
    return result

# typical use in a unittest:
# class releaseTest(h2o_common.ReleaseCommon, unittest.TestCase):
# see multiple inheritance at http://docs.python.org/release/1.5/tut/node66.html
#************************************************************************************
class SetupOneJVM14(object):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=1, java_heap_GB=14)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # if we got here by time out exception waiting for a job, we should clear
        # all jobs, if we're leaving h2o cloud up, and going to run another test
        #h2o.cancelAllJobs() 
        h2o.tear_down_cloud()

#************************************************************************************
class SetupThreeJVM4(object):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(node_count=3, java_heap_GB=4)
        else:
            h2o_hosts.build_cloud_with_hosts()

    @classmethod
    def tearDownClass(cls):
        # if we got here by time out exception waiting for a job, we should clear
        # all jobs, if we're leaving h2o cloud up, and going to run another test
        #h2o.cancelAllJobs() 
        h2o.tear_down_cloud()

#************************************************************************************
class ReleaseCommon(object):
    def tearDown(self):
        print "tearDown"
        # try to download the logs...may fail again! If we have no logs, h2o_sandbox will complain about not being able to look at anything
        h2o.nodes[0].log_download()
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        print "setUpClass"
        # standard method for being able to reproduce the random.* seed
        h2o.setup_random_seed()
        # do a hack so we can run release tests with a -cj arg. so we don't have to build the cloud separately
        # those tests will always want to run non-local (big machien) so detecting -cj is fine
        if h2o.config_json:
            h2o_hosts.build_cloud_with_hosts()
        else:
            # this is the normal thing for release tests (separate cloud was built. clone it)
            h2o.build_cloud_with_json()
        # if you're fast with a test and cloud building, you may need to wait for cloud to stabilize
        # normally this is baked into build_cloud, but let's leave it here for now
        h2o.stabilize_cloud(h2o.nodes[0], h2o.nodes, timeoutSecs=90)
        # this ?should? work although not guaranteed all will agree on the cloud size
        # unless we do the conservative stabilize above
        h2o.verify_cloud_size()

    @classmethod
    def tearDownClass(cls):
        print "tearDownClass"
        # DON"T
        ### h2o.tear_down_cloud()

        # this could fail too
        if h2o.nodes[0].delete_keys_at_teardown:
            start = time.time()
            h2i.delete_keys_at_all_nodes(timeoutSecs=300)
            elapsed = time.time() - start
            print "delete_keys_at_all_nodes(): took", elapsed, "secs"

#************************************************************************************
# no log download or key delete. Used for the test_shutdown.py
class ReleaseCommon2(object):
    def tearDown(self):
        print "tearDown"
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        print "setUpClass"
        h2o.build_cloud_with_json()
        # normally this shouldn't be necessary?
        h2o.stabilize_cloud(h2o.nodes[0], h2o.nodes, timeoutSecs=90)


#************************************************************************************
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
#************************************************************************************
