import time, sys
import h2o, h2o_browse as h2b

# poll the Jobs queue and wait if not all done. 
# Return matching keys to a pattern for 'destination_key"
# for a job (model usually)

# FIX! the pattern doesn't limit the jobs you wait for (sounds like it does)
# I suppose it's rare that we'd want to wait for a subset of jobs, but lets 
# 'key' 'description' 'destination_key' could all be interesting things you want to pattern match agains?
# what the heck, just look for a match in any of the 3 (no regex)
# if pattern is not None, only stall on jobs that match the pattern (in any of those 3)
def pollWaitJobs(pattern=None, timeoutSecs=30, pollTimeoutSecs=30, retryDelaySecs=5, benchmarkLogging=None, stallForNJobs=None):
    wait = True
    waitTime = 0
    ignoredJobs = set()
    while (wait):
        a = h2o.nodes[0].jobs_admin(timeoutSecs=pollTimeoutSecs)
        ## print "jobs_admin():", h2o.dump_json(a)
        jobs = a['jobs']
        busy = 0
        for j in jobs:
            ### h2o.verboseprint(j)
            if j['end_time'] == '':
                if not pattern: 
                    print "description:", j['description'], "end_time:", j['end_time']
                    busy +=1
                    h2o.verboseprint("pollWaitJobs: found a busy job, now: %s" % busy)
                else:
                    if (pattern in j['key']) or (pattern in j['destination_key']) or (pattern in j['description']):
                        print "description:", j['description'], "end_time:", j['end_time']
                        busy += 1
                        h2o.verboseprint("pollWaitJobs: found a pattern-matched busy job, now %s" % busy)
                    # we only want to print the warning message once
                    elif j['key'] not in ignoredJobs:
                        jobMsg = "%s %s %s" % (j['key'], j['description'], j['destination_key'])
                        print " %s job in progress but we're ignoring it. Doesn't match pattern." % jobMsg
                        # I guess "key" is supposed to be unique over all time for a job id?
                        ignoredJobs.add(j['key'])

            elif waitTime: # we've been waiting
                h2o.verboseprint("waiting", waitTime, "secs, still not done - ",\
                "destination_key:", j['destination_key'], \
                "progress:",  j['progress'], \
                "cancelled:", j['cancelled'],\
                "end_time:",  j['end_time'])

        if stallForNJobs:
            waitFor = stallForNJobs
        else:
            waitFor = 0

        print " %s jobs in progress." % busy, "Waiting until %s in progress." % waitFor
        wait = busy > waitFor
        if not wait:
            break

        ### h2b.browseJsonHistoryAsUrlLastMatch("Jobs")
        if (wait and waitTime > timeoutSecs):
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

        # check the sandbox for stack traces! just like we do when polling normally
        h2o.check_sandbox_for_errors()

    patternKeys = []
    for j in jobs:
        # save the destination keys in progress that match pattern (for returning)
        if pattern and pattern in j['destination_key']:
            patternKeys.append(j['destination_key'])

    return patternKeys


#*******************************************************************************************
def cancelAllJobs(timeoutSecs=10, **kwargs): # I guess you could pass pattern
    # what if jobs had just been dispatched? wait until they get in the queue state correctly
    time.sleep(2)
    a = h2o.nodes[0].jobs_admin(timeoutSecs=120)
    print "jobs_admin():", h2o.dump_json(a)
    jobsList = a['jobs']
    for j in jobsList:
        if j['end_time'] == '':
            b = h2o.nodes[0].jobs_cancel(key=j['key'])
            print "jobs_cancel():", h2o.dump_json(b)

    # it's possible we could be in a bad state where jobs don't cancel cleanly
    pollWaitJobs(timeoutSecs=timeoutSecs, **kwargs) # wait for all the cancels to happen. If we missed one, we might timeout here.
