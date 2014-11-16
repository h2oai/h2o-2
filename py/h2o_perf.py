import logging, psutil
import h2o
import time, os, re
import h2o_nodes
from h2o_test import dump_json, verboseprint

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

    def switch_logfile(self, location, log):
        #similar to change_logfile, but not for python subtests
        #used in h2o/bench/BMscripts/* e.g.
        #location is either an absolute path or a subdirectory
        #no trailing slashes for location
        #no leading slashes for log and no suffix
        location = location.strip('/')
        log = re.sub("\.[a-z]*","",log)
        blog = location + "/" + log + ".log"
        print "\nSwitch log file; appending to %s." %blog, "Between tests, you may want to delete it if it gets too big"
        
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
        self.IOSTATSINTERVAL = 10

        # initialize state used for spot rate measurements during polling
        statsList = ['read_bytes','write_bytes','read_time','write_time',
            'bytes_sent','bytes_recv','dropin','dropout','errin','errout']
        self.pollStats = {}
        for s in statsList:
            self.pollStats[s] = 0
        self.pollStats['count'] = 0

        self.snapshotTime = time.time()
        self.pollStats['lastJstackTime'] = self.snapshotTime
        self.pollStats['lastIOstatsTime'] = self.snapshotTime
        self.pollStats['time'] = self.snapshotTime

        self.elapsedTime = 0

    def save(self, cpu_percent=None, dioc=None, nioc=None, jstack=None, iostats=None, snapshotTime=None):
        # allow incremental update, or all at once
        if cpu_percent:
            self.pollStats['cpu_percent'] = cpu_percent
        if dioc:
            self.pollStats['read_bytes'] =  dioc.read_bytes
            self.pollStats['write_bytes'] =  dioc.write_bytes
            self.pollStats['read_time'] =  dioc.read_time
            self.pollStats['write_time'] =  dioc.write_time
        if nioc:
            self.pollStats['bytes_sent'] =  nioc.bytes_sent
            self.pollStats['bytes_recv'] =  nioc.bytes_recv
            if 1==1: # bad on some?
                self.pollStats['dropin'] =  nioc.dropin
                self.pollStats['dropout'] =  nioc.dropout
                self.pollStats['errin'] =  nioc.errin
                self.pollStats['errout'] =  nioc.errout
        if jstack:
            self.pollStats['lastJstackTime'] = self.snapshotTime
        if iostats:
            self.pollStats['lastIOstatsTime'] = self.snapshotTime
        # this guy is the 'final'
        if snapshotTime:
            self.pollStats['time'] = self.snapshotTime
            self.pollStats['count'] += 1

    # just log a message..useful for splitting tests of files 
    def message(self, l):
        logging.critical(l)
      
    def log_jstack(self, initOnly=False):
        # only do jstack if >= JSTACKINTERVAL seconds since lastLine one
        if ((self.snapshotTime - self.pollStats['lastJstackTime']) < self.JSTACKINTERVAL):
            return

        # complicated because it's all one big string 
        # and lots of info we don't want. 
        jstackResult = h2o_nodes.nodes[0].jstack()
        node0 = jstackResult['nodes'][0]
        stack_traces = node0["traces"]
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
        self.pollStats['lastJstackTime'] = self.snapshotTime
        self.save(jstack=True)

    def log_cpu(self, snapShotTime, initOnly=False):
        cpu_percent = psutil.cpu_percent(percpu=True)
        l = "%s %s" % ("cpu_percent:", cpu_percent)
        if not initOnly:
            logging.critical(l)
        self.save(cpu_percent=cpu_percent)

    def log_disk(self, initOnly=False):
        dioc = psutil.disk_io_counters()
        diocSpotRdMBSec = (dioc.read_bytes - self.pollStats['read_bytes']) / (1e6 * self.elapsedTime)
        diocSpotWrMBSec = (dioc.write_bytes - self.pollStats['write_bytes']) / (1e6 * self.elapsedTime)
        diocSpotRdTime = (dioc.read_time - self.pollStats['read_time']) / 1e3
        diocSpotWrTime = (dioc.write_time - self.pollStats['write_time']) / 1e3
        l = "Disk. Spot RdMB/s: {:>6.2f} Spot WrMB/s: {:>6.2f} {!s} {!s} elapsed: {:<6.2f}".format(
            diocSpotRdMBSec, diocSpotWrMBSec, diocSpotRdTime, diocSpotWrTime, self.elapsedTime)
        if not initOnly:
            logging.critical(l)
        self.save(dioc=dioc)

    def log_network(self, initOnly=False):
        nioc = psutil.network_io_counters()
        niocSpotSentMBSec = (nioc.bytes_sent - self.pollStats['bytes_sent'])/(1e6 * self.elapsedTime)
        niocSpotRecvMBSec = (nioc.bytes_recv - self.pollStats['bytes_recv'])/(1e6 * self.elapsedTime)
        if 1==1: # some don't work but we'll enable here
            niocSpotDropIn = nioc.dropin -  self.pollStats['dropin']
            niocSpotDropOut = nioc.dropout -  self.pollStats['dropout']
            niocSpotErrIn = nioc.errin -  self.pollStats['errin']
            niocSpotErrOut = nioc.errout -  self.pollStats['errout']
        else:
            # stuff doesn't exist on ec2?
            niocSpotDropIn = 0
            niocSpotDropOut = 0
            niocSpotErrIn = 0
            niocSpotErrOut = 0

        l = "Network. Spot RecvMB/s: {:>6.2f} Spot SentMB/s: {:>6.2f} {!s} {!s} {!s} {!s}".format(
            niocSpotRecvMBSec, niocSpotSentMBSec,\
            niocSpotDropIn, niocSpotDropOut, niocSpotErrIn, niocSpotErrOut)
        if not initOnly:
            logging.critical(l)
        self.save(nioc=nioc)

    def log_iostats(self, initOnly=False):
        if ((self.snapshotTime - self.pollStats['lastJstackTime']) < self.IOSTATSINTERVAL):
            return

        DO_IO_RW = True
        DO_IOP = True
        DO_BLOCKED = False

        node = h2o_nodes.nodes[0]
        stats = node.iostatus()
        ### verboseprint("log_iostats:", dump_json(stats))
        histogram = stats['histogram']

        def log_window(k,w):
            ## in case the window disappears from h2o, print what's available with this line
            ## print k['window']
            if k['window'] == w:
                i_o = k['i_o']
                node = k['cloud_node_idx']
                if k['r_w'] == 'read':
                    r_w = 'rd'
                elif k['r_w'] == 'write':
                    r_w = 'wr'
                else: 
                    r_w = k['r_w']
                
                for l,v in k.iteritems():
                    fmt = "iostats: window{:<2d} node {:d} {:<4s} {:s} {:s} MB/sec: {:6.2f}"
                    if 'peak' in l:
                        ## logging.critical(fmt.format(w, node, i_o, r_w, "peak", (v/1e6)))
                        pass
                    if 'effective' in l:
                        logging.critical(fmt.format(w, node, i_o, r_w, "eff.", (v/1e6)))
                return True
            else:   
                return False # not found

        if DO_IO_RW:
            print "\nlog_iotstats probing node:", str(node.addr) + ":" + str(node.port)
            found = False
            for k in histogram:
                ### print k
                found |= log_window(k,60)
                ### log_window(30)
            if not found:
                print "iostats: desired window not found in histogram"
                # 1 5 60 300 available

        # we want to sort the results before we print them, so grouped by node
        if DO_IOP:
            iopList = []
            raw_iops = stats['raw_iops']
            ### print
            for k in raw_iops:
                ### print k
                node = k['node']
                i_o = k['i_o']
                r_w = k['r_w']
                size = k['size_bytes']
                blocked = k['blocked_ms']
                duration = k['duration_ms']
                if duration != 0:
                    blockedPct = "%.2f" % (100 * blocked/duration) + "%"
                else:
                    blockedPct = "no duration"
                iopMsg = "node: %s %s %s %d bytes. blocked: %s" % (node, i_o, r_w, size, blockedPct)
                # FIX! don't dump for now
                iopList.append([node, iopMsg])

            iopList.sort(key=lambda iop: iop[0])  # sort by node
            totalSockets = len(iopList)
            # something wrong if 0?
            if totalSockets == 0:
                print "WARNING: is something wrong with this io stats response?"
                print dump_json(stats)

            logging.critical("iostats: " + "Total sockets: " + str(totalSockets))
            if DO_BLOCKED:
                for i in iopList:
                    logging.critical("iostats:" + i[1])

        # don't save anything
        self.save(iostats=True)
    
    # call with init?
    def get_log_save(self, benchmarkLogging=None, initOnly=False):
        if not benchmarkLogging:
            return
        self.snapshotTime = time.time()
        self.elapsedTime = self.snapshotTime - self.pollStats['time']

        logEnable = {
            'cpu': False,
            'disk': False,
            'network': False,
            'jstack': False,
            'iostats': False,
        }
        for e in benchmarkLogging:
            logEnable[e] = True

        if logEnable['jstack']:
            self.log_jstack(initOnly=initOnly)
        if logEnable['cpu']:
            self.log_cpu(initOnly)
        if logEnable['iostats']:
            self.log_iostats(initOnly=initOnly)
        # these do delta stats. force init if no delta possible
        forceInit = self.pollStats['count'] == 0
        if logEnable['disk']:
            self.log_disk(initOnly=initOnly or forceInit)
        if logEnable['network']:
            self.log_network(initOnly=initOnly or forceInit)

        # done!
        self.save(snapshotTime=True)

