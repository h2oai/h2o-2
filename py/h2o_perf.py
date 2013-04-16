import logging, psutil
import h2o
import time, os

class PerfH2O(object):
    # so a test can create multiple logs
    def change_logfile(self, subtest_name):
        # change to another logfile after we've already been going
        # just want the base name if we pointed to it from somewhere else
        short_subtest_name = os.path.basename(subtest_name) 
        blog = 'benchmark_' + short_subtest_name + '.log'
        print "\nSwitch. Now appending to %s." % blog, "Between tests, you may want to delete it if it gets too big"

        # http://stackoverflow.com/questions/5296130/restart-logging-to-a-new-file-python
        # manually reassign the handler
        logger = logging.getLogger()
        logger.handlers[0].stream.close()
        logger.removeHandler(logger.handlers[0])

        file_handler = logging.FileHandler(blog)
        file_handler.setLevel(logging.CRITICAL) # like the init
        formatter = logging.Formatter("%(asctime)s %(message)s") # date/time stamp
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    def init_logfile(self, subtest_name):
        # default should just append thru multiple cloud builds.
        # I guess sandbox is cleared on each cloud build. so don't build there.
        # just use local directory? (python_test_name global set below before this)
        short_subtest_name = os.path.basename(subtest_name) 
        blog = 'benchmark_' + short_subtest_name + '.log'
        self.subtest_name = short_subtest_name
        print "\nAppending to %s." % blog, "Between tests, you may want to delete it if it gets too big"
        logging.basicConfig(filename=blog,
            # we use CRITICAL for the benchmark logging to avoid info/warn stuff
            # from other python packages
            level=logging.CRITICAL,
            format='%(asctime)s %(message)s') # date/time stamp

    def __init__(self, python_test_name):
        short_python_test_name = os.path.basename(python_test_name) 
        self.python_test_name = short_python_test_name
        self.init_logfile(short_python_test_name)

        self.MINCACHETOPRINT = 7
        self.JSTACKINTERVAL = 20

        # initialize state used for spot rate measurements during polling
        statsList = ['read_bytes','write_bytes','read_time','write_time',
            'bytes_sent','bytes_recv','dropin','dropout','errin','errout']
        self.pollStats = {}
        for s in statsList:
            self.pollStats[s] = 0
        self.pollStats['count'] = 0
        self.pollStats['lastJstackTime'] = time.time()

    # split out the get and save, so we can 'get_save' to initialize values 
    # for delta measurements. The first delta will be a bit off, 
    # because elapsed isn't accurate time since the initial setting.
    def get(self):
        cpu_percent = psutil.cpu_percent(percpu=True)
        dioc = psutil.disk_io_counters()
        nioc = psutil.network_io_counters()
        snapshotTime = time.time()
        return (cpu_percent, dioc, nioc, snapshotTime)

    def save(self, cpu_percent, dioc, nioc, snapshotTime):
        self.pollStats['cpu_percent'] = cpu_percent
        self.pollStats['read_bytes'] =  dioc.read_bytes
        self.pollStats['write_bytes'] =  dioc.write_bytes
        self.pollStats['read_time'] =  dioc.read_time
        self.pollStats['write_time'] =  dioc.write_time
        self.pollStats['bytes_sent'] =  nioc.bytes_sent
        self.pollStats['bytes_recv'] =  nioc.bytes_recv
        # self.pollStats['dropin'] =  nioc.dropin
        # self.pollStats['dropout'] =  nioc.dropout
        # self.pollStats['errin'] =  nioc.errin
        # self.pollStats['errout'] =  nioc.errout
        self.pollStats['count'] += 1
        self.pollStats['time'] = snapshotTime

      
    def get_save(self):
        (cpu_percent, dioc, nioc, snapshotTime) = self.get()
        self.save(cpu_percent, dioc, nioc, snapshotTime)

    def get_log_save(self, benchmarkLogging=None):
        (cpu_percent, dioc, nioc, snapshotTime) = self.get()

        pollStats = self.pollStats
        elapsedTime = snapshotTime - pollStats['time']

        logEnable = {
            'cpu': False,
            'disk': False,
            'network': False,
            'jstack': False,
        }
        for e in benchmarkLogging:
            logEnable[e] = True

        # only do jstack if >= JSTACKINTERVAL seconds since lastLine one
        if logEnable['jstack'] and ((snapshotTime - pollStats['lastJstackTime']) >= self.JSTACKINTERVAL):
            # okay..get some Jstack stuff, and mark it as gathered now
            self.pollStats['lastJstackTime'] = snapshotTime
            
            # complicated because it's all one big string 
            # and lots of info we don't want. 
            jstackResult = h2o.nodes[0].jstack()
            node0 = jstackResult['nodes'][0]
            stack_traces = node0["stack_traces"]
            # all one string
            stackLines = stack_traces.split('\n')

            # create cache
            def init_cache(self):
                self.cache = []
                self.cacheHasJstack = False
                self.cacheHasTCP = False

            def log_and_init_cache(self):
                if self.cacheHasTCP or (not self.cacheHasJstack and len(self.cache) >= self.MINCACHETOPRINT):
                    for c in self.cache:
                        logging.critical(c)
                init_cache(self)

            init_cache(self)
            # pretend to start at stack trace break
            lastLine = ""
            for s in stackLines:
                # look for gaps, if 7 lines in your cache, print them
                if (lastLine==""): 
                    log_and_init_cache(self)
                else:
                    # put a nice "#" char for grepping out jstack stuff
                    self.cache.append("#" + s)
                    # always throw it away later if JStack cache
                    if 'JStack' in s:
                        self.cacheHasJstack = True
                    # always print it if it mentions TCP
                    if 'TCP' in s:
                        self.cacheHasTCP = True
                lastLine = s

            # check last one
            log_and_init_cache(self)

        l = "%s %s" % ("cpu_percent:", cpu_percent)
        if logEnable['cpu']:
            logging.critical(l)

        diocSpotRdMBSec = (dioc.read_bytes - pollStats['read_bytes']) / (1e6 * elapsedTime)
        diocSpotWrMBSec = (dioc.write_bytes - pollStats['write_bytes']) / (1e6 * elapsedTime)
        diocSpotRdTime = (dioc.read_time - pollStats['read_time']) / 1e3
        diocSpotWrTime = (dioc.write_time - pollStats['write_time']) / 1e3
        l = "Disk. Spot RdMB/s: {:<6.2f} Spot WrMB/s: {:<6.2f} {!s} {!s} elapsed: {:<6.2f}".format(
            diocSpotRdMBSec, diocSpotWrMBSec, diocSpotRdTime, diocSpotWrTime, elapsedTime)
        if logEnable['disk'] and pollStats['count'] > 0:
            logging.critical(l)

        niocSpotSentMBSec = (nioc.bytes_sent - pollStats['bytes_sent'])/(1e6 * elapsedTime)
        niocSpotRecvMBSec = (nioc.bytes_recv - pollStats['bytes_recv'])/(1e6 * elapsedTime)
        # niocSpotDropIn = nioc.dropin -  pollStats['dropin']
        # niocSpotDropOut = nioc.dropout -  pollStats['dropout']
        # niocSpotErrIn = nioc.errin -  pollStats['errin']
        # niocSpotErrOut = nioc.errout -  pollStats['errout']

        # stuff doesn't exist on ec2?
        niocSpotDropIn = 0
        niocSpotDropOut = 0
        niocSpotErrIn = 0
        niocSpotErrOut = 0
        l = "Network. Spot RecvMB/s: {:<6.2f} Spot SentMB/s: {:<6.2f} {!s} {!s} {!s} {!s}".format(
            niocSpotRecvMBSec, niocSpotSentMBSec,\
            niocSpotDropIn, niocSpotDropOut, niocSpotErrIn, niocSpotErrOut)
        if logEnable['network'] and pollStats['count'] > 0:
            logging.critical(l)

        self.save(cpu_percent, dioc, nioc, snapshotTime)

    # just logg a message..useful for splitting tests of files 
    def message(self, l):
        logging.critical(l)

