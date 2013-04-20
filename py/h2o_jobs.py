import time, sys
import h2o
import h2o_browse as h2b

# poll the Jobs queue and wait if not all done. Return matching keys to a pattern for 'destination_key"
# for a job (model usually)
def pollWaitJobs(pattern=None, timeoutSecs=30, pollTimeoutSecs=30, retryDelaySecs=5, benchmarkLogging=None):
    anyBusy = True
    waitTime = 0
    while (anyBusy):
        # timeout checking has to move in here now! just count loops
        anyBusy = False
        a = h2o.nodes[0].jobs_admin(timeoutSecs=pollTimeoutSecs)
        ## print "jobs_admin():", h2o.dump_json(a)
        jobs = a['jobs']
        patternKeys = []
        for j in jobs:
            ### h2o.verboseprint(j)
            # save the destination keys for any GLMModel in progress
            if pattern and pattern in j['destination_key']:
                patternKeys.append(j['destination_key'])

            if j['end_time'] == '':
                anyBusy = True
                h2o.verboseprint("waiting", waitTime, "secs, still not done - ",\
                    "destination_key:", j['destination_key'], \
                    "progress:",  j['progress'], \
                    "cancelled:", j['cancelled'],\
                    "end_time:",  j['end_time'])

        h2b.browseJsonHistoryAsUrlLastMatch("Jobs")
        if (anyBusy and waitTime > timeoutSecs):
            print h2o.dump_json(jobs)
            raise Exception("Some queued jobs haven't completed after", timeoutSecs, "seconds")

        sys.stdout.write('.')
        sys.stdout.flush()
        time.sleep(retryDelaySecs)
        waitTime += retryDelaySecs

        # any time we're sitting around polling we might want to save logging info (cpu/disk/jstack)
        # test would pass ['cpu','disk','jstack'] kind of list
        if benchmarkLogging:
            h2o.cloudPerfH2O.get_log_save(benchmarkLogging)
    return patternKeys


