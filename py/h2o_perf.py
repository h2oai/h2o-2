import logging, psutil

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
      
    def get_and_log(self, retryDelaySecs, network=False, cpu=True, disk=True):
        cpup = psutil.cpu_percent(percpu=True)
        l = "%s %s %s" % (self.python_test_name, "psutil.cpu_percent:", cpup)
        if cpu:
            logging.critical(l)

        pollStats = self.pollStats

        dioc = psutil.disk_io_counters()
        diocSpotRdMBSec = (dioc.read_bytes - pollStats['read_bytes']) / (1e6 * retryDelaySecs)
        diocSpotWrMBSec = (dioc.write_bytes - pollStats['write_bytes']) / (1e6 * retryDelaySecs)
        pollStats['read_bytes'] =  dioc.read_bytes
        pollStats['write_bytes'] =  dioc.write_bytes
        diocSpotRdTime = (dioc.read_time - pollStats['read_time']) / 1e3
        diocSpotWrTime = (dioc.write_time - pollStats['write_time']) / 1e3
        pollStats['read_time'] =  dioc.read_time
        pollStats['write_time'] =  dioc.write_time
        l = "Disk. Spot RdMB/s:{:6.2f} Spot WrMB/s:{:6.2f} {!s} {!s}".format(
            diocSpotRdMBSec, diocSpotWrMBSec, diocSpotRdTime, diocSpotWrTime)
        if disk and pollStats['count'] > 0:
            logging.critical(l)

        nioc = psutil.network_io_counters()
        niocSpotSentMBSec = (nioc.bytes_sent - pollStats['bytes_sent'])/(1e6 * retryDelaySecs)
        niocSpotRecvMBSec = (nioc.bytes_recv - pollStats['bytes_recv'])/(1e6 * retryDelaySecs)
        pollStats['bytes_sent'] =  nioc.bytes_sent
        pollStats['bytes_recv'] =  nioc.bytes_recv

        niocSpotDropIn = nioc.dropin -  pollStats['dropin']
        niocSpotDropOut = nioc.dropout -  pollStats['dropout']
        niocSpotErrIn = nioc.errin -  pollStats['errin']
        niocSpotErrOut = nioc.errout -  pollStats['errout']
        pollStats['dropin'] =  nioc.dropin
        pollStats['dropout'] =  nioc.dropout
        pollStats['errin'] =  nioc.errin
        pollStats['errout'] =  nioc.errout

        l = "Network. Spot RecvMB/s:{:6.2f} Spot SentMB/s:{:6.2f} {!s} {!s} {!s} {!s}".format(
            niocSpotRecvMBSec, niocSpotSentMBSec,\
            niocSpotDropIn, niocSpotDropOut, niocSpotErrIn, niocSpotErrOut)
        if network and pollStats['count'] > 0:
            logging.critical(l)

        self.pollStats['count'] += 1
        self.pollStats = pollStats

