import logging, psutil
import h2o
import time

class PerfH2O(object):
    def __init__(self, python_test_name):
        # default should just append thru multiple cloud builds.
        # I guess sandbox is cleared on each cloud build. so don't build there.
        # just use local directory? (python_test_name global set below before this)
        blog = 'benchmark_' + python_test_name + '.log'
        self.python_test_name = python_test_name
        print "Appending to %s." % blog, "Between tests, you may want to delete it if it gets too big"
        logging.basicConfig(filename=blog,
            # we use CRITICAL for the benchmark logging to avoid info/warn stuff
            # from other python packages
            level=logging.CRITICAL,
            format='%(asctime)s %(message)s') # date/time stamp

        # initialize state used for spot rate measurements during polling
        statsList = ['read_bytes','write_bytes','read_time','write_time',
            'bytes_sent','bytes_recv','dropin','dropout','errin','errout']
        self.pollStats = {}
        for s in statsList:
            self.pollStats[s] = 0
        self.pollStats['count'] = 0
        self.pollStats['lastJstackTime'] = time.time()

    # split out the get and save, to initialize values for delta measurements
    # the first will be a bit off, because elapsed isn't accurate time since the initial setting.
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
        self.pollStats['dropin'] =  nioc.dropin
        self.pollStats['dropout'] =  nioc.dropout
        self.pollStats['errin'] =  nioc.errin
        self.pollStats['errout'] =  nioc.errout
        self.pollStats['count'] += 1
        self.pollStats['time'] = snapshotTime

      
    def get_save(self):
        (cpu_percent, dioc, nioc, snapshotTime) = self.get()
        self.save(cpu_percent, dioc, nioc, snapshotTime)

    def get_log_save(self, benchmarkLogging=None):
        (cpu_percent, dioc, nioc, snapshotTime) = self.get()

        pollStats = self.pollStats
        elapsedTime = snapshotTime - pollStats['time']

        enable = {
            'cpu': False,
            'disk': False,
            'network': False,
            'jstack': False,
        }
        for e in benchmarkLogging:
            enable[e] = True

        # only do jstack if >= 10 seconds since last one
        if enable['jstack'] and ((snapshotTime - pollStats['lastJstackTime']) >= 10):
            self.pollStats['lastJstackTime'] = snapshotTime
            
            # complicated because it's all bad string 
            # and lots of info we don't want. 
            # just print stack traces > a number of "lines"
            stats = h2o.nodes[0].jstack()
            node0 = stats['nodes'][0]
            stack_traces = node0["stack_traces"]
            # all one string
            slines = stack_traces.split('\n')

            MINCACHETOPRINT = 7
            cache = []
            jstackCache = False
            last = ""

            def check_cache():
                if not jstackCache and len(cache) >= MINCACHETOPRINT:
                    for c in cache:
                        logging.critical(c)

            for s in slines:
                # look for gaps, if 7 lines in your cache, print them
                if (last==""): 
                    check_cache()
                    cache = []
                    jstackCache = False
                else:
                    cache.append(s)
                    # throw it away later if JStack cache
                    if 'JStack' in s:
                        jstackCache = True
                    
                last = s
            # check last one
            check_cache()

        l = "%s %s" % ("cpu_percent:", cpu_percent)
        if enable['cpu']:
            logging.critical(l)


        diocSpotRdMBSec = (dioc.read_bytes - pollStats['read_bytes']) / (1e6 * elapsedTime)
        diocSpotWrMBSec = (dioc.write_bytes - pollStats['write_bytes']) / (1e6 * elapsedTime)
        diocSpotRdTime = (dioc.read_time - pollStats['read_time']) / 1e3
        diocSpotWrTime = (dioc.write_time - pollStats['write_time']) / 1e3
        l = "Disk. Spot RdMB/s:{:6.2f} Spot WrMB/s:{:6.2f} {!s} {!s}".format(
            diocSpotRdMBSec, diocSpotWrMBSec, diocSpotRdTime, diocSpotWrTime)
        if enable['disk'] and pollStats['count'] > 0:
            logging.critical(l)

        niocSpotSentMBSec = (nioc.bytes_sent - pollStats['bytes_sent'])/(1e6 * elapsedTime)
        niocSpotRecvMBSec = (nioc.bytes_recv - pollStats['bytes_recv'])/(1e6 * elapsedTime)
        niocSpotDropIn = nioc.dropin -  pollStats['dropin']
        niocSpotDropOut = nioc.dropout -  pollStats['dropout']
        niocSpotErrIn = nioc.errin -  pollStats['errin']
        niocSpotErrOut = nioc.errout -  pollStats['errout']
        l = "Network. Spot RecvMB/s:{:6.2f} Spot SentMB/s:{:6.2f} {!s} {!s} {!s} {!s}".format(
            niocSpotRecvMBSec, niocSpotSentMBSec,\
            niocSpotDropIn, niocSpotDropOut, niocSpotErrIn, niocSpotErrOut)
        if enable['network'] and pollStats['count'] > 0:
            logging.critical(l)

        self.save(cpu_percent, dioc, nioc, snapshotTime)

    # just logg a message..useful for splitting tests of files 
    def message(self, l):
        logging.critical(message)

