import unittest, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o_cmd, h2o, h2o_browse as h2b, h2o_os_util

node_count = 5
portsPerNode = 2

def check_cloud_and_setup_next():
    h2b.browseTheCloud()
    h2o.verify_cloud_size()
    h2o.check_sandbox_for_errors()
    print "Tearing down cloud of size", len(h2o.nodes)
    h2o.tear_down_cloud()
    # this will delete the flatfile in sandbox
    h2o.clean_sandbox()
    # wait to make sure no sticky ports or anything os-related
    # so let's expand the delay if larger number of jvms
    # 1 second per node seems good
    h2o.verboseprint("Waiting", node_count, "seconds to avoid OS sticky port problem")
    time.sleep(node_count)
    # stick port issues (os)
    # wait a little for jvms to really clear out?
    # if we change the port we have to upload the flatfile again
    # maybe just use_flatfile=false

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        # do the first one to build up hosts
        # so we don't repeatedly copy the jar
        start = time.time()
        # writes it's own flatfile
        h2o.init(node_count, use_flatfile=True, java_heap_GB=1)
        print "jar/flatfile copied and Cloud of", len(h2o.nodes), "built in", time.time()-start, "seconds"
        # have to remember total # of nodes for the next class. it will stay the same
        # when we tear down the cloud, we zero the nodes list
        global totalNodes
        totalNodes = len(h2o.nodes)
        check_cloud_and_setup_next()

    @classmethod
    def tearDownClass(cls):
        pass

    def test_remote_cloud(self):
        # FIX! we should increment this from 1 to N? 
        for i in range(1,10):
            # timeout wants to be larger for large numbers of hosts * node_count
            # don't want to reload jar/flatfile, so use build cloud
            timeoutSecs = max(60, 8 * totalNodes)
            print "totalNodes:", totalNodes, "timeoutSecs:", timeoutSecs

            # FIX! ..just use_flatfile=False for now on these subsequent ones. rely on multicast
            # Could make the biggest cloud first and count down? supposed to not matter if flatfile has too many?
            start = time.time()
            # it should know not to upload the jars again
            # the flatfile was deleted, so we can't do build cloud with hosts defined..it assumed flatfile was created for upload
            h2o.init(node_count, use_flatfile=False, timeoutSecs=timeoutSecs, retryDelaySecs=0.5)
            print "Cloud of", len(h2o.nodes), "built in", time.time()-start, "seconds"
            check_cloud_and_setup_next()


if __name__ == '__main__':
    h2o.unit_main()

