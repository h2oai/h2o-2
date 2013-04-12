import os, json, unittest, time, shutil, sys
sys.path.extend(['.','..','py'])

import h2o, h2o_cmd, h2o_glm, h2o_hosts
import h2o_browse as h2b

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):
        global localhost
        localhost = h2o.decide_if_localhost()
        if (localhost):
            h2o.build_cloud(3)
        else:
            h2o_hosts.build_cloud_with_hosts(3)

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()

    def test_GLM_big1_nopoll(self):
        csvPathname = h2o.find_file("smalldata/hhp_107_01.data.gz")
        print "\n" + csvPathname

        y = "106"
        x = ""
        parseKey = h2o_cmd.parseFile(csvPathname=csvPathname, timeoutSecs=15)

        glmInitial = []
        # dispatch multiple jobs back to back
        for jobDispatch in range(40):

            start = time.time()
            kwargs = {'x': x, 'y': y, 'n_folds': 1}
            # FIX! what model keys do these get?
            glm = h2o_cmd.runGLMOnly(parseKey=parseKey, timeoutSecs=300, noPoll=True, **kwargs)
            glmInitial.append(glm)
            print "glm job dispatch end on ", csvPathname, 'took', time.time() - start, 'seconds'
            print "\njobDispatch #", jobDispatch

        # FIX! need to get more intelligent here
        
        anyBusy = True
        waitLoop = 0
        while (anyBusy):
            # timeout checking has to move in here now! just count loops
            anyBusy = False
            a = h2o.nodes[0].jobs_admin()
            ## print "jobs_admin():", h2o.dump_json(a)
            jobs = a['jobs']
            GLMModelKeys = []
            for j in jobs:
                # save the destination keys for any GLMModel in progress
                if 'GLMModel' in j['destination_key']:
                    GLMModelKeys.append(j['destination_key'])

                if j['end_time'] == '':
                    anyBusy = True
                    print "Loop", waitLoop, "Not done - ",\
                        "destination_key:", j['destination_key'], \
                        "progress:",  j['progress'], \
                        "cancelled:", j['cancelled'],\
                        "end_time:",  j['end_time']
            print "\n"
            h2b.browseJsonHistoryAsUrlLastMatch("Jobs")
            if (anyBusy and waitLoop > 20):
                print h2o.dump_json(jobs)
                raise Exception("Some queued jobs haven't completed after", waitLoop, "wait loops")
            
            time.sleep(5)
            waitLoop += 1

            
        # b = h2o.nodes[0].jobs_cancel(key='pytest_model')
        # print "jobs_cancel():", h2o.dump_json(b)

        # we saved the initial response?
        # if we do another poll they should be done now, and better to get it that 
        # way rather than the inspect (to match what simpleCheckGLM is expected
        for glm in glmInitial:
            print "Checking completed job, with no polling:", glm
            a = h2o.nodes[0].poll_url(glm['response'], noPoll=True)
            h2o_glm.simpleCheckGLM(self, a, 57, **kwargs)

if __name__ == '__main__':
    h2o.unit_main()
