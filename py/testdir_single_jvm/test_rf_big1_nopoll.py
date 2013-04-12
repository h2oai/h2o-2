import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_rf, h2o_hosts
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(1)
        else:
            h2o_hosts.build_cloud_with_hosts(1)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_rf_big1_nopoll(self):
        csvPathname = h2o.find_file("smalldata/hhp_107_01.data.gz")
        print "\n" + csvPathname

        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, timeoutSecs=15)

        rfViewInitial = []
        # dispatch multiple jobs back to back
        for jobDispatch in range(1):

            start = time.time()
            kwargs = {}
            # FIX! what model keys do these get?
            rfView = h2o_cmd.runRFOnly(parseKey=parseKey, model_key="RF_model"+str(jobDispatch),\
                timeoutSecs=300, noPoll=True, **kwargs)
            rfViewInitial.append(rfView)
            print "rf job dispatch end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "\njobDispatch #", jobDispatch

        anyBusy = True
        waitLoop = 0
        while (anyBusy):
            anyBusy = False
            a = h2o.nodes[0].jobs_admin()
            print "jobs_admin():", h2o.dump_json(a)
            jobs = a['jobs']
            RFModelKeys = []
            for j in jobs:
                # save the destination keys for any RF Model in progress
                if 'RF' in j['destination_key']:
                   RFModelKeys.append(j['destination_key'])

                if j['end_time'] == '':
                    anyBusy = True
                    print "Loop", waitLoop, "Not done - ",\
                        "destination_key:", j['destination_key'], \
                        "progress:",  j['progress'], \
                        "cancelled:", j['cancelled'],\
                        "end_time:",  j['end_time']
            print "\n"
            h2b.browseJsonHistoryAsUrlLastMatch("Jobs")
            if (anyBusy and waitLoop > 2):
                print h2o.dump_json(jobs)
                raise Exception("Some queued jobs haven't completed after", waitLoop, "wait loops")

            time.sleep(5)
            waitLoop += 1

            
        # b = h2o.nodes[0].jobs_cancel(key='pytest_model')
        # print "jobs_cancel():", h2o.dump_json(b)

        # we saved the initial response?
        # if we do another poll they should be done now, and better to get it that 
        # way rather than the inspect (to match what simpleCheckGLM is expected
        for rfView in rfViewInitial:
            print "Checking completed job, with no polling:", rfView
            a = h2o.nodes[0].poll_url(rf['response'], noPoll=True)
            h2o_rf.simpleCheckRFView(None, a)

if __name__ == '__main__':
    h2o.unit_main()
