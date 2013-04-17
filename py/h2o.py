import time, os, json, signal, tempfile, shutil, datetime, inspect, threading, os.path, getpass
import requests, psutil, argparse, sys, unittest
import glob
import h2o_browse as h2b
import h2o_perf

import re
import webbrowser
import random
# used in shutil.rmtree permission hack for windows
import errno
# use to unencode the urls sent to h2o?
import urlparse
import logging

# For checking ports in use, using netstat thru a subprocess.
from subprocess import Popen, PIPE

# The cloud is uniquely named per user (only)
# Fine to uniquely identify the flatfile by name only also?
# Both are the user that runs the test. The config might have a different username on the
# remote machine (0xdiag, say, or hduser)
def flatfile_name():
    return('pytest_flatfile-%s' %getpass.getuser())

def cloud_name():
    return('pytest-%s-%s' % (getpass.getuser(), os.getpid()))
    # return('pytest-%s' % getpass.getuser())

def __drain(src, dst):
    for l in src:
        if type(dst) == type(0):
            os.write(dst, l)
        else:
            dst.write(l)
            dst.flush()
    src.close()
    if type(dst) == type(0):
        os.close(dst)

def drain(src, dst):
    t = threading.Thread(target=__drain, args=(src,dst))
    t.daemon = True
    t.start()

def unit_main():
    global python_test_name
    python_test_name = inspect.stack()[1][1]
    print "\nRunning: python", python_test_name
    # moved clean_sandbox out of here, because nosetests doesn't execute h2o.unit_main in our tests.
    # UPDATE: ..is that really true? I'm seeing the above print in the console output runnning
    # jenkins with nosetests
    parse_our_args()
    unittest.main()

# Global disable. used to prevent browsing when running nosetests, or when given -bd arg
# Defaults to true, if user=jenkins, h2o.unit_main isn't executed, so parse_our_args isn't executed.
# Since nosetests doesn't execute h2o.unit_main, it should have the browser disabled.
browse_disable = True
browse_json = False
verbose = False
ipaddr = None
config_json = None
debugger = False
random_udp_drop = False
# jenkins gets this assign, but not the unit_main one?
python_test_name = inspect.stack()[1][1]

def parse_our_args():
    parser = argparse.ArgumentParser()
    # can add more here
    parser.add_argument('-bd', '--browse_disable', help="Disable any web browser stuff. Needed for batch. nosetests and jenkins disable browser through other means already, so don't need", action='store_true')
    parser.add_argument('-b', '--browse_json', help='Pops a browser to selected json equivalent urls. Selective. Also keeps test alive (and H2O alive) till you ctrl-c. Then should do clean exit', action='store_true')
    parser.add_argument('-v', '--verbose', help='increased output', action='store_true')
    parser.add_argument('-ip', '--ip', type=str, help='IP address to use for single host H2O with psutil control')
    parser.add_argument('-cj', '--config_json', help='Use this json format file to provide multi-host defaults. Overrides the default file pytest_config-<username>.json. These are used only if you do build_cloud_with_hosts()')
    parser.add_argument('-dbg', '--debugger', help='Launch java processes with java debug attach mechanisms', action='store_true')
    parser.add_argument('-rud', '--random_udp_drop', help='Drop 20 pct. of the UDP packets at the receive side', action='store_true')
    parser.add_argument('unittest_args', nargs='*')

    args = parser.parse_args()
    global browse_disable, browse_json, verbose, ipaddr, config_json, debugger, random_udp_drop

    browse_disable = args.browse_disable or getpass.getuser()=='jenkins'
    browse_json = args.browse_json
    verbose = args.verbose
    ipaddr = args.ip
    config_json = args.config_json
    debugger = args.debugger
    random_udp_drop = args.random_udp_drop

    # Set sys.argv to the unittest args (leav sys.argv[0] as is)
    # FIX! this isn't working to grab the args we don't care about
    # Pass "--failfast" to stop on first error to unittest. and -v
    # won't get this for jenkins, since it doesn't do parse_our_args
    sys.argv[1:] = ['-v', "--failfast"] + args.unittest_args
    # sys.argv[1:] = args.unittest_args

def verboseprint(*args, **kwargs):
    if verbose:
        for x in args: # so you don't have to create a single string
            print x,
        for x in kwargs: # so you don't have to create a single string
            print x,
        print
        # so we can see problems when hung?
        sys.stdout.flush()

def find_dataset(f):
    (head, tail) = os.path.split(os.path.abspath('datasets'))
    verboseprint("find_dataset looking upwards from", head, "for", tail)
    # don't spin forever 
    levels = 0
    while not (os.path.exists(os.path.join(head, tail))):
        head = os.path.split(head)[0]
        levels += 1
        if (levels==10): 
            raise Exception("unable to find datasets. Did you git it?")

    return os.path.join(head, tail, f)

def find_file(base):
    f = base
    if not os.path.exists(f): f = '../' + base
    if not os.path.exists(f): f = '../../' + base
    if not os.path.exists(f): f = 'py/' + base
    if not os.path.exists(f):
        raise Exception("unable to find file %s" % base)
    return f

# Return file size.
def get_file_size(f):
    return os.path.getsize(f)

# Splits file into chunks of given size and returns an iterator over chunks.
def iter_chunked_file(file, chunk_size=2048):
    return iter(lambda: file.read(chunk_size), '')

# shutil.rmtree doesn't work on windows if the files are read only.
# On unix the parent dir has to not be readonly too.
# May still be issues with owner being different, like if 'system' is the guy running?
# Apparently this escape function on errors is the way shutil.rmtree can 
# handle the permission issue. (do chmod here)
# But we shouldn't have read-only files. So don't try to handle that case.
def handleRemoveError(func, path, exc):
    # If there was an error, it could be due to windows holding onto files.
    # Wait a bit before retrying. Ignore errors on the retry. Just leave files.
    # Ex. if we're in the looping cloud test deleting sandbox.
    excvalue = exc[1]
    print "Retrying shutil.rmtree of sandbox (2 sec delay). Will ignore errors. Exception was", excvalue.errno
    time.sleep(2)
    try:
        func(path)
    except OSError:
        pass

LOG_DIR = 'sandbox'
def clean_sandbox():
    if os.path.exists(LOG_DIR):
        # shutil.rmtree fails to delete very long filenames on Windoze
        #shutil.rmtree(LOG_DIR)
        # was this on 3/5/13. This seems reliable on windows+cygwin
        ### os.system("rm -rf "+LOG_DIR)
        shutil.rmtree(LOG_DIR, ignore_errors=False, onerror=handleRemoveError)
    # it should have been removed, but on error it might still be there
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

# who knows if this one is ok with windows...doesn't rm dir, just 
# the stdout/stderr files
def clean_sandbox_stdout_stderr():
    if os.path.exists(LOG_DIR):
        files = []
        # glob.glob returns an iterator
        for f in glob.glob(LOG_DIR + '/*stdout*'):
            verboseprint("cleaning", f)
            os.remove(f)
        for f in glob.glob(LOG_DIR + '/*stderr*'):
            verboseprint("cleaning", f)
            os.remove(f)

def tmp_file(prefix='', suffix=''):
    return tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=LOG_DIR)
def tmp_dir(prefix='', suffix=''):
    return tempfile.mkdtemp(prefix=prefix, suffix=suffix, dir=LOG_DIR)

def log(cmd, comment=None):
    with open(LOG_DIR + '/commands.log', 'a') as f:
        f.write(str(datetime.datetime.now()) + ' -- ')
        # what got sent to h2o
        # f.write(cmd)
        # let's try saving the unencoded url instead..human readable
        f.write(urlparse.unquote(cmd))
        if comment:
            f.write('    #')
            f.write(comment)
        f.write("\n")

def make_syn_dir():
    SYNDATASETS_DIR = './syn_datasets'
    if os.path.exists(SYNDATASETS_DIR):
        shutil.rmtree(SYNDATASETS_DIR)
    os.mkdir(SYNDATASETS_DIR)
    return SYNDATASETS_DIR

def dump_json(j):
    return json.dumps(j, sort_keys=True, indent=2)

# Hackery: find the ip address that gets you to Google's DNS
# Trickiness because you might have multiple IP addresses (Virtualbox), or Windows.
# we used to not like giving ip 127.0.0.1 to h2o?
def get_ip_address():
    if ipaddr:
        verboseprint("get_ip case 1:", ipaddr)
        return ipaddr

    import socket
    ip = '127.0.0.1'
    # this method doesn't work if vpn is enabled..it gets the vpn ip
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8',0))
        ip = s.getsockname()[0]
        verboseprint("get_ip case 2:", ip)
    except:
        pass

    if ip.startswith('127'):
        ip = socket.getaddrinfo(socket.gethostname(), None)[0][4][0]
        verboseprint("get_ip case 3:", ip)

    ipa = None
    for ips in socket.gethostbyname_ex(socket.gethostname())[2]:
         # only take the first 
         if ipa is None and not ips.startswith("127."):
            ipa = ips[:]
            verboseprint("get_ip case 4:", ipa)
            if ip != ipa:
                print "\nAssuming", ip, "is the ip address h2o will use but", ipa, "is probably the real ip?"
                print "You might have a vpn active. Best to use '-ip "+ipa+"' to get python and h2o the same."

    verboseprint("get_ip_address:", ip) 
    return ip

def spawn_cmd(name, args, capture_output=True):
    if capture_output:
        outfd,outpath = tmp_file(name + '.stdout.', '.log')
        errfd,errpath = tmp_file(name + '.stderr.', '.log')
        ps = psutil.Popen(args, stdin=None, stdout=outfd, stderr=errfd)
    else:
        outpath = '<stdout>'
        errpath = '<stderr>'
        ps = psutil.Popen(args)

    comment = 'PID %d, stdout %s, stderr %s' % (
        ps.pid, os.path.basename(outpath), os.path.basename(errpath))
    log(' '.join(args), comment=comment)
    return (ps, outpath, errpath)

def spawn_cmd_and_wait(name, args, timeout=None):
    (ps, stdout, stderr) = spawn_cmd(name, args)

    rc = ps.wait(timeout)
    out = file(stdout).read()
    err = file(stderr).read()

    if rc is None:
        ps.terminate()
        raise Exception("%s %s timed out after %d\nstdout:\n%s\n\nstderr:\n%s" %
                (name, args, timeout or 0, out, err))
    elif rc != 0:
        raise Exception("%s %s failed.\nstdout:\n%s\n\nstderr:\n%s" % (name, args, out, err))

# used to get a browser pointing to the last RFview
global json_url_history
json_url_history = []

global nodes
nodes = []

# I suppose we could shuffle the flatfile order!
# but it uses hosts, so if that got shuffled, we got it covered?
# the i in xrange part is not shuffled. maybe create the list first, for possible random shuffle
# FIX! default to random_shuffle for now..then switch to not.
def write_flatfile(node_count=2, base_port=54321, hosts=None, rand_shuffle=True):
    # always create the flatfile. 
    ports_per_node = 2
    pff = open(flatfile_name(), "w+")
    # doing this list outside the loops so we can shuffle for better test variation
    hostPortList = []
    if hosts is None:
        ip = get_ip_address()
        for i in range(node_count):
            hostPortList.append("/" + ip + ":" + str(base_port + ports_per_node*i))
    else:
        for h in hosts:
            for i in range(node_count):
                hostPortList.append("/" + h.addr + ":" + str(base_port + ports_per_node*i))

    # note we want to shuffle the full list of host+port
    if rand_shuffle: 
        random.shuffle(hostPortList)
    for hp in hostPortList:
        pff.write(hp + "\n")
    pff.close()


def check_port_group(base_port):
    # for now, only check for jenkins or kevin
    if (1==1):
        username = getpass.getuser()
        if username=='jenkins' or username=='kevin' or username=='michal':
            # assumes you want to know about 3 ports starting at base_port
            command1Split = ['netstat', '-anp']
            command2Split = ['egrep']
            # colon so only match ports. space at end? so no submatches
            command2Split.append("(%s | %s)" % (base_port, base_port+1) )
            command3Split = ['wc','-l']

            print "Checking 2 ports starting at ", base_port
            print ' '.join(command2Split)

            # use netstat thru subprocess
            p1 = Popen(command1Split, stdout=PIPE)
            p2 = Popen(command2Split, stdin=p1.stdout, stdout=PIPE)
            output = p2.communicate()[0]
            print output

def default_hosts_file():
    return 'pytest_config-{0}.json'.format(getpass.getuser())

# node_count is number of H2O instances per host if hosts is specified.
def decide_if_localhost():
    # First, look for local hosts file
    hostsFile = default_hosts_file()
    if config_json:
        print "* Using config JSON you passed as -cj argument:", config_json
        return False
    if os.path.exists(hostsFile): 
        print "* Using matching username config JSON file discovered in this directory: {0}.".format(hostsFile)
        return False
    if 'hosts' in os.getcwd():
        print "Since you're in a *hosts* directory, we're using a config json"
        print "* Expecting default username's config json here. Better exist!"
        return False
    print "No config json used. Launching local cloud..."
    return True

# node_count is per host if hosts is specified.
def build_cloud(node_count=2, base_port=54321, hosts=None, 
        timeoutSecs=30, retryDelaySecs=0.5, cleanup=True, rand_shuffle=True, **kwargs):
    # moved to here from unit_main. so will run with nosetests too!
    clean_sandbox()
    # keep this param in kwargs, because we pass to the H2O node build, so state
    # is created that polling and other normal things can check, to decide to dump 
    # info to benchmark.log
    if kwargs.setdefault('enable_benchmark_log', False):
        # an object to keep stuff out of h2o.py        
        global cloudPerfH2O
        cloudPerfH2O = h2o_perf.PerfH2O(python_test_name)

    ports_per_node = 2 
    node_list = []
    try:
        # if no hosts list, use psutil method on local host.
        totalNodes = 0
        # doing this list outside the loops so we can shuffle for better test variation
        # this jvm startup shuffle is independent from the flatfile shuffle
        portList = [base_port + ports_per_node*i for i in range(node_count)]
        if hosts is None:
            # if use_flatfile, we should create it, 
            # because tests will just call build_cloud with use_flatfile=True
            # best to just create it all the time..may or may not be used 
            write_flatfile(node_count=node_count, base_port=base_port)
            hostCount = 1
            if rand_shuffle: random.shuffle(portList)
            for p in portList:
                verboseprint("psutil starting node", i)
                newNode = LocalH2O(port=p, node_id=totalNodes, **kwargs)
                node_list.append(newNode)
                totalNodes += 1
        else:
            # if hosts, the flatfile was created and uploaded to hosts already
            # I guess don't recreate it, don't overwrite the one that was copied beforehand.
            # we don't always use the flatfile (use_flatfile=False)
            # Suppose we could dispatch from the flatfile to match it's contents
            # but sometimes we want to test with a bad/different flatfile then we invoke h2o?
            hostCount = len(hosts)
            hostPortList = []
            for h in hosts:
                for port in portList:
                    hostPortList.append( (h,port) )
            if rand_shuffle: random.shuffle(hostPortList)
            for (h,p) in hostPortList:
                verboseprint('ssh starting node', totalNodes, 'via', h)
                newNode = h.remote_h2o(port=p, node_id=totalNodes, **kwargs)
                node_list.append(newNode)
                totalNodes += 1

        verboseprint("Attempting Cloud stabilize of", totalNodes, "nodes on", hostCount, "hosts")
        start = time.time()
        # UPDATE: best to stabilize on the last node!
        stabilize_cloud(node_list[-1], len(node_list), 
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)
        verboseprint(len(node_list), "Last added node stabilized in ", time.time()-start, " secs")
        verboseprint("Built cloud: %d node_list, %d hosts, in %d s" % (len(node_list), 
            hostCount, (time.time() - start))) 

        # FIX! using "consensus" in node[-1] should mean this is unnecessary?
        # maybe there's a bug. For now do this. long term: don't want?
        # UPDATE: do it for all cases now 2/14/13
        for n in node_list:
            stabilize_cloud(n, len(node_list), timeoutSecs=timeoutSecs)
        # best to check for any errors due to cloud building right away?
        check_sandbox_for_errors()

    except:
        if cleanup:
            for n in node_list: n.terminate()
        else:
            nodes[:] = node_list
        check_sandbox_for_errors()
        raise

    # this is just in case they don't assign the return to the nodes global?
    nodes[:] = node_list
    print len(node_list), "total jvms in H2O cloud"
    return node_list

def upload_jar_to_remote_hosts(hosts, slow_connection=False):
    def prog(sofar, total):
        # output is bad for jenkins. 
        username = getpass.getuser()
        if username!='jenkins':
            p = int(10.0 * sofar / total)
            sys.stdout.write('\rUploading jar [%s%s] %02d%%' % ('#'*p, ' '*(10-p), 100*sofar/total))
            sys.stdout.flush()
        
    if not slow_connection:
        for h in hosts:
            f = find_file('target/h2o.jar')
            h.upload_file(f, progress=prog)
            # skipping progress indicator for the flatfile
            h.upload_file(flatfile_name())
    else:
        f = find_file('target/h2o.jar')
        hosts[0].upload_file(f, progress=prog)
        hosts[0].push_file_to_remotes(f, hosts[1:])

        f = find_file(flatfile_name())
        hosts[0].upload_file(f, progress=prog)
        hosts[0].push_file_to_remotes(f, hosts[1:])

def check_sandbox_for_errors(sandbox_ignore_errors=False):
    if not os.path.exists(LOG_DIR):
        return
    # dont' have both tearDown and tearDownClass report the same found error
    # only need the first
    if nodes and nodes[0].sandbox_error_report():
        return
    # FIX! wait for h2o to flush to files? how?
    # Dump any assertion or error line to the screen
    # Both "passing" and failing tests??? I guess that's good.
    # if you find a problem, just keep printing till the end, in that file. 
    # The stdout/stderr is shared for the entire cloud session?
    # so don't want to dump it multiple times?
    errLines = []
    for filename in os.listdir(LOG_DIR):
        if re.search('stdout|stderr',filename):
            sandFile = open(LOG_DIR + "/" + filename, "r")
            # just in case error/assert is lower or upper case
            # FIX! aren't we going to get the cloud building info failure messages
            # oh well...if so ..it's a bug! "killing" is temp to detect jar mismatch error
            regex1 = re.compile(
                'found multiple|exception|error|assert|killing|killed|required ports',
                re.IGNORECASE)
            regex2 = re.compile('Caused',re.IGNORECASE)
            regex3 = re.compile('warn|info|TCP', re.IGNORECASE)

            # there are many hdfs/apache messages with error in the text. treat as warning if they have '[WARN]'
            # i.e. they start with:
            # [WARN]

            # if we started due to "warning" ...then if we hit exception, we don't want to stop
            # we want that to act like a new beginning. Maybe just treat "warning" and "info" as
            # single line events? that's better
            printing = 0 # "printing" is per file. 
            lines = 0 # count per file! errLines accumulates for multiple files.
            for line in sandFile:
                # JIT reporting looks like this..don't detect that as an error
                printSingleWarning = False
                foundBad = False
                if not ' bytes)' in line:
                    # no multiline FSM on this 
                    printSingleWarning = regex3.search(line) and not ('[Loaded ' in line)
                    #   13190  280      ###        sun.nio.ch.DatagramChannelImpl::ensureOpen (16 bytes)

                    # don't detect these class loader info messags as errors
                    #[Loaded java.lang.Error from /usr/lib/jvm/java-7-oracle/jre/lib/rt.jar]
                    foundBad = regex1.search(line) and not (
                        ('error rate' in line) or ('[Loaded ' in line) or ('[WARN]' in line))

                if (printing==0 and foundBad):
                    printing = 1
                    lines = 1
                elif (printing==1):
                    lines += 1
                    # if we've been printing, stop when you get to another error
                    # keep printing if the pattern match for the condition
                    # is on a line with "Caused" in it ("Caused by")
                    # only use caused for overriding an end condition
                    foundCaused = regex2.search(line)
                    # since the "at ..." lines may have the "bad words" in them, we also don't want 
                    # to stop if a line has " *at " at the beginning.
                    # Update: Assertion can be followed by Exception. 
                    # Make sure we keep printing for a min of 4 lines
                    foundAt = re.match(r'[\t ]+at ',line)
                    if foundBad and (lines>10) and not (foundCaused or foundAt):
                        printing = 2 

                if (printing==1):
                    # to avoid extra newline from print. line already has one
                    errLines.append(line)
                    sys.stdout.write(line)

                if (printSingleWarning):
                    # don't print this one
                    if not re.search("Unable to load native-hadoop library for your platform", line):
                        sys.stdout.write(line)

            sandFile.close()
    sys.stdout.flush()

    # already has \n in each line
    # doing this kludge to put multiple line message in the python traceback, 
    # so it will be reported by jenkins. The problem with printing it to stdout
    # is that we're in the tearDown class, and jenkins won't have this captured until
    # after it thinks the test is done (tearDown is separate from the test)
    # we probably could have a tearDown with the test rather than the class, but we
    # would have to update all tests. 
    if len(errLines)!=0:
        # check if the lines all start with INFO: or have "apache" in them
        justInfo = True
        for e in errLines:
            justInfo &= re.match("INFO:", e) or ("apache" in e)

        if not justInfo:
            emsg1 = " check_sandbox_for_errors: Errors in sandbox stdout or stderr.\n" + \
                     "Could have occurred at any prior time\n\n"
            emsg2 = "".join(errLines)
            if nodes: 
                nodes[0].sandbox_error_report(True)

            # can build a cloud that ignores all sandbox things that normally fatal the test
            # kludge, test will set this directly if it wants, rather than thru build_cloud
            # parameter. 
            # we need the sandbox_ignore_errors, for the test teardown_cloud..the state 
            # disappears!
            if sandbox_ignore_errors or (nodes and nodes[0].sandbox_ignore_errors):
                pass
            else:
                raise Exception(python_test_name + emsg1 + emsg2)

def tear_down_cloud(node_list=None, sandbox_ignore_errors=False):
    if not node_list: node_list = nodes
    try:
        for n in node_list:
            n.terminate()
            verboseprint("tear_down_cloud n:", n)
    finally:
        check_sandbox_for_errors(sandbox_ignore_errors=sandbox_ignore_errors)
        node_list[:] = []

# don't need any more? 
# Used before to make sure cloud didn't go away between unittest defs
def touch_cloud(node_list=None):
    if not node_list: node_list = nodes
    for n in node_list:
        n.is_alive()

def verify_cloud_size():
    expectedSize = len(nodes)
    cloudSizes = [n.get_cloud()['cloud_size'] for n in nodes]
    cloudConsensus = [n.get_cloud()['consensus'] for n in nodes]
    for s in cloudSizes:
        consensusStr = (",".join(map(str,cloudConsensus)))
        sizeStr =   (",".join(map(str,cloudSizes)))
        if (s != expectedSize):
            raise Exception("Inconsistent cloud size." + 
                "nodes report size: %s consensus: %s instead of %d." % \
                (sizeStr, consensusStr, expectedSize))
    return (sizeStr, consensusStr, expectedSize)
    
def stabilize_cloud(node, node_count, timeoutSecs=14.0, retryDelaySecs=0.25):
    node.wait_for_node_to_accept_connections(timeoutSecs)
    # want node saying cloud = expected size, plus thinking everyone agrees with that.
    def test(n, tries=None):
        c = n.get_cloud()
        # don't want to check everything. But this will check that the keys are returned!
        consensus  = c['consensus']
        locked     = c['locked']
        cloud_size = c['cloud_size']
        cloud_name = c['cloud_name']
        node_name  = c['node_name']

        if 'nodes' not in c:
            emsg = "\nH2O didn't include a list of nodes in get_cloud response after initial cloud build"
            raise Exception(emsg)
        cnodes     = c['nodes'] # list of dicts 
        if (cloud_size > node_count):
            print "\nNodes in current cloud:"
            for c in cnodes:
                print c['name']
        
            emsg = (
                "\n\nERROR: cloud_size: %d reported via json is bigger than we expect: %d" % (cloud_size, node_count) +
                "\nYou likely have zombie(s) with the same cloud name on the network, that's forming up with you." +
                "\nLook at the cloud IP's in 'grep Paxos sandbox/*stdout*' for some IP's you didn't expect." +
                "\n\nYou probably don't have to do anything, as the cloud shutdown in this test should"  +
                "\nhave sent a Shutdown.json to all in that cloud (you'll see a kill -2 in the *stdout*)." +
                "\nIf you try again, and it still fails, go to those IPs and kill the zombie h2o's." +
                "\nIf you think you really have an intermittent cloud build, report it." +
                "\n" +
                "\nUPDATE: building cloud size of 2 with 127.0.0.1 may temporarily report 3 incorrectly, with no zombie?" 
                )
            raise Exception(emsg)
            print emsg

        
        a = (cloud_size==node_count) and consensus
        if a:
            verboseprint("\tLocked won't happen until after keys are written")
            verboseprint("\nNodes in current cloud:")
            for c in cnodes:
                verboseprint(c['name'])

        return a

    node.stabilize(test, error=('A cloud of size %d' % node_count),
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)

class H2O(object):
    def __url(self, loc, port=None):
        # always use the new api port
        if port is None: port = self.port
        u = 'http://%s:%d/%s' % (self.http_addr, port, loc)
        return u 

    def __check_request(self, r, extraComment=None, ignoreH2oError=False):
        if extraComment:
            log('Sent ' + r.url + " # " + extraComment)
        else:
            log('Sent ' + r.url)

        # fatal if no response
        if not r: 
            raise Exception("Maybe bad url? no r in __check_request %s in %s:" % (e, inspect.stack()[1][3]))

        # this is used to open a browser on results, or to redo the operation in the browser
        # we don't' have that may urls flying around, so let's keep them all
        json_url_history.append(r.url)
        if not r.json():
            raise Exception("Maybe bad url? no r.json in __check_request %s in %s:" % (e, inspect.stack()[1][3]))
            
        rjson = r.json()

        for e in ['error', 'Error', 'errors', 'Errors']:
            if e in rjson:
                verboseprint(dump_json(rjson))
                emsg = 'rjson %s in %s: %s' % (e, inspect.stack()[1][3], rjson[e])
                if ignoreH2oError:
                    # well, we print it..so not totally ignore. test can look at rjson returned
                    print emsg
                else:
                    raise Exception(emsg)

        for w in ['warning', 'Warning', 'warnings', 'Warnings']:
            if w in rjson:
                verboseprint(dump_json(rjson))
                print 'rjson %s in %s: %s' % (w, inspect.stack()[1][3], rjson[w])

        return rjson


    def test_redirect(self):
        return self.__check_request(requests.get(self.__url('TestRedirect.json')))
    def test_poll(self, args):
        return self.__check_request(requests.get(
                    self.__url('TestPoll.json'),
                    params=args))

    def get_cloud(self):
        a = self.__check_request(requests.get(self.__url('Cloud.json')))
        consensus  = a['consensus']
        locked     = a['locked']
        cloud_size = a['cloud_size']
        cloud_name = a['cloud_name']
        node_name  = a['node_name']
        node_id    = self.node_id
        verboseprint('%s%s %s%s %s%s %s%s' %(
            "\tnode_id: ", node_id,
            "\tcloud_size: ", cloud_size,
            "\tconsensus: ", consensus,
            "\tlocked: ", locked,
            ))
        return a

    def get_timeline(self):
        return self.__check_request(requests.get(self.__url('Timeline.json')))

    # Shutdown url is like a reset button. Doesn't send a response before it kills stuff
    # safer if random things are wedged, rather than requiring response
    # so request library might retry and get exception. allow that.
    def shutdown_all(self):
        try:
            self.__check_request(requests.get(self.__url('Shutdown.json')))
        except:
            pass
        return(True)

    def put_value(self, value, key=None, repl=None):
        return self.__check_request(
            requests.get(
                self.__url('PutValue.json'), 
                params={"value": value, "key": key, "replication_factor": repl}),
            extraComment = str(value) + "," + str(key) + "," + str(repl))

    def put_file(self, f, key=None, timeoutSecs=60):
        if key is None:
            key = os.path.basename(f)
            ### print "putfile specifying this key:", key

        resp = self.__check_request(
            requests.post(
                self.__url('PostFile.json'),
                timeout=timeoutSecs,
                params={"key": key},
                files={"file": open(f, 'rb')}),
            extraComment = str(f))

        verboseprint("\nput_file response: ", dump_json(resp))
        return key
    
    def get_key(self, key):
        return requests.get(self.__url('Get.html'),
            params={"key": key})

    # noise is a 2-tuple ("StoreView", none) for url plus args for doing during poll to create noise
    # so we can create noise with different urls!, and different parms to that url
    # no noise if None
    def poll_url(self, response, 
        timeoutSecs=10, retryDelaySecs=0.5, initialDelaySecs=None, pollTimeoutSecs=15,
        noise=None, benchmarkLogging=None, noPoll=False):
        ### print "poll_url: pollTimeoutSecs", pollTimeoutSecs 
        verboseprint('poll_url input: response:', dump_json(response))

        url = self.__url(response['redirect_request'])
        params = response['redirect_request_args']

        if noise is not None:
            print noise
            # noise_json should be like "Storeview"
            (noise_json, noiseParams) = noise
            noiseUrl = self.__url(noise_json + ".json")

        status = 'poll'
        r = {} # response

        start = time.time()
        count = 0
        # FIX! temporarily wait 5x the retryDelaySecs delay, before the first poll
        # Progress status NPE issue (H2O)
        if initialDelaySecs:
            time.sleep(initialDelaySecs)

        # can end with status = 'redirect' or 'done'
        # FIX! temporary hack ...if a GLMModel key shows up, treat that as "stop polling'   
        # because we have results for GLm. (i.e. ignore status.
        while status == 'poll':
            # UPDATE: 1/24/13 change to always wait before the first poll..
            # see if it makes a diff to our low rate fails
            time.sleep(retryDelaySecs)
            # every other one?
            create_noise = noise is not None and ((count%2)==0)
            if create_noise:
                urlUsed = noiseUrl
                paramsUsed = noiseParams
                msgUsed = "\nNoise during polling with"
            else:
                urlUsed = url
                paramsUsed = params
                msgUsed = "\nPolling with"

            r = self.__check_request(
                requests.get(
                    url=urlUsed,
                    timeout=pollTimeoutSecs, 
                    params=paramsUsed))

            if ((count%5)==0):
                verboseprint(msgUsed, urlUsed, "Response:", dump_json(r['response']))
            # hey, check the sandbox if we've been waiting a long time...rather than wait for timeout
            # to find the badness?
            if ((count%15)==0):
                check_sandbox_for_errors()

            if (create_noise):
                # this guarantees the loop is done, so we don't need to worry about 
                # a 'return r' being interpreted from a noise response
                status = 'poll'
            else:
                status = r['response']['status']

            if ((time.time()-start)>timeoutSecs):
                # show what we're polling with 
                argsStr =  '&'.join(['%s=%s' % (k,v) for (k,v) in paramsUsed.items()])
                emsg = "Exceeded timeoutSecs: %d secs while polling." % timeoutSecs +\
                       "status: %s, url: %s?%s" % (status, urlUsed, argsStr)
                raise Exception(emsg)
            count += 1

            if noPoll:
                return r
            # GLM can return partial results during polling..that's legal
            ### if 'GLMProgressPage' in urlUsed and 'GLMModel' in r:
            ###    print "INFO: GLM returning partial results during polling. Continuing.."

            if benchmarkLogging:
                cloudPerfH2O.get_log_save(benchmarkLogging)

        return r
    
    # additional params include: cols=. 
    # don't need to include in params_dict it doesn't need a default
    def kmeans(self, key, key2=None, 
        timeoutSecs=300, retryDelaySecs=0.2, initialDelaySecs=None, pollTimeoutSecs=30,
        **kwargs):
        # defaults
        params_dict = {
            'epsilon': 1e-6,
            'k': 1,
            'source_key': key,
            'destination_key': None,
            }
        if key2 is not None: params_dict['destination_key'] = key2
        params_dict.update(kwargs)
        print "\nKMeans params list", params_dict
        a = self.__check_request(
            requests.get(
                url=self.__url('KMeans.json'),
                timeout=timeoutSecs,
                params=params_dict))

        # Check that the response has the right Progress url it's going to steer us to.
        if a['response']['redirect_request']!='Progress':
            print dump_json(a)
            raise Exception('H2O kmeans redirect is not Progress. KMeans json response precedes.')
        a = self.poll_url(a['response'],
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, 
            initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
        verboseprint("\nKMeans result:", dump_json(a))
        return a

    # params: 
    # header=1, 
    # separator=1 (hex encode?
    # exclude=
    # noise is a 2-tuple: ("StoreView",params_dict)
    
    def parse(self, key, key2=None, 
        timeoutSecs=300, retryDelaySecs=0.2, initialDelaySecs=None, pollTimeoutSecs=30,
        noise=None, benchmarkLogging=False, noPoll=False, **kwargs):
        browseAlso = kwargs.pop('browseAlso',False)
        # this doesn't work. webforums indicate max_retries might be 0 already? (as of 3 months ago)
        # requests.defaults({max_retries : 4})
        # https://github.com/kennethreitz/requests/issues/719
        # it was closed saying Requests doesn't do retries. (documentation implies otherwise)
        verboseprint("\nParsing key:", key, "to key2:", key2, "(if None, means default)")

        # other h2o parse parameters, not in the defauls
        # header
        # exclude
        params_dict = {
            'source_key': key, # can be a regex
            'destination_key': key2,
            }
        params_dict.update(kwargs)

        if benchmarkLogging:
            cloudPerfH2O.get_save()

        a = self.__check_request(
            requests.get(
                url=self.__url('Parse.json'),
                timeout=timeoutSecs,
                params=params_dict))

        # Check that the response has the right Progress url it's going to steer us to.
        if a['response']['redirect_request']!='Progress':
            print dump_json(a)
            raise Exception('H2O parse redirect is not Progress. Parse json response precedes.')

        if noPoll:
            return a

        # noise is a 2-tuple ("StoreView, none) for url plus args for doing during poll to create noise
        # no noise if None
        verboseprint('Parse.Json noise:', noise)
        a = self.poll_url(a['response'],
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, 
            initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
            noise=noise, benchmarkLogging=benchmarkLogging)
        verboseprint("\nParse result:", dump_json(a))
        return a

    def netstat(self):
        return self.__check_request(requests.get(self.__url('Network.json')))

    def jstack(self):
        return self.__check_request(requests.get(self.__url("JStack.json")))

    # &offset=
    # &view=
    def inspect(self, key, offset=None, view=None, ignoreH2oError=False):
        a = self.__check_request(requests.get(self.__url('Inspect.json'),
            params={
                "key": key,
                "offset": offset,
                "view": view,
                }),
            ignoreH2oError=ignoreH2oError
            )
        # too much!
        ### verboseprint("\ninspect result:", dump_json(a))
        return a

    # FIX! what params does this take
    def store_view(self):
        a = self.__check_request(requests.get(self.__url('StoreView.json'),
            params={}))
        # too much!
        ### verboseprint("\ninspect result:", dump_json(a))
        return a

    # There is also a RemoveAck in the browser, that asks for confirmation from
    # the user. This is after that confirmation.
    def remove_key(self, key):
        a = self.__check_request(requests.get(self.__url('Remove.json'),
            params={"key": key}))

        # too much!
        ### verboseprint("\ninspect result:", dump_json(a))
        return a

    # H2O doesn't support yet?
    def Store2HDFS(self, key):
        a = self.__check_request(requests.get(self.__url('Store2HDFS.json'),
            params={"key": key}))
        verboseprint("\ninspect result:", dump_json(a))
        return a

    # ImportFiles replaces ImportFolder, with a param that can be a folder or a file.
    # the param name is 'file', but it can take a directory or a file.
    # 192.168.0.37:54323/ImportFiles.html?file=%2Fhome%2F0xdiag%2Fdatasets
    # Can import just a file or a whole folder
    def import_files(self, path):
        a = self.__check_request(requests.get(
            self.__url('ImportFiles.json'),
            params={ "path": path}))
        verboseprint("\nimport_files result:", dump_json(a))
        return a

    def import_s3(self, bucket):
        a = self.__check_request(requests.get(
            self.__url('ImportS3.json'),
            params={"bucket": bucket}))
        verboseprint("\nimport_s3 result:", dump_json(a))
        return a

    def import_hdfs(self, path):
        a = self.__check_request(requests.get(
            self.__url('ImportHdfs.json'),
            params={ "path": path}))
        verboseprint("\nimport_hdfs result:", dump_json(a))
        return a

    # 'destination_key', 'escape_nan' 'expression'
    def exec_query(self, timeoutSecs=20, **kwargs):
        params_dict = {
            'expression': None,
            }
        browseAlso = kwargs.pop('browseAlso',False)
        params_dict.update(kwargs)
        verboseprint("\nexec_query:", params_dict)
        a = self.__check_request(requests.get(
            url=self.__url('Exec.json'),
            timeout=timeoutSecs,
            params=params_dict))
        verboseprint("\nexec_query result:", dump_json(a))
        return a

    def jobs_admin(self, timeoutSecs=20, **kwargs):
        params_dict = {
            # 'expression': None,
            }
        browseAlso = kwargs.pop('browseAlso',False)
        params_dict.update(kwargs)
        verboseprint("\nexec_query:", params_dict)
        a = self.__check_request(requests.get(
            url=self.__url('Jobs.json'),
            timeout=timeoutSecs,
            params=params_dict))
        verboseprint("\njobs_admin result:", dump_json(a))
        return a

    def jobs_cancel(self, timeoutSecs=20, **kwargs):
        params_dict = {
            # 'expression': None,
            }
        browseAlso = kwargs.pop('browseAlso',False)
        params_dict.update(kwargs)
        verboseprint("\nexec_query:", params_dict)
        a = self.__check_request(requests.get(
            url=self.__url('Cancel.json'),
            timeout=timeoutSecs,
            params=params_dict))
        verboseprint("\njobs_cancel result:", dump_json(a))
        return a


    # note ntree in kwargs can overwrite trees! (trees is legacy param)
    def random_forest(self, data_key, trees, timeoutSecs=300, print_params=True, **kwargs):
        params_dict = {
            'data_key': data_key,
            'ntree':  trees,
            'model_key': 'pytest_model',
            # new default. h2o defaults to 0, better for tracking oobe problems
            'out_of_bag_error_estimate': 1, 
            }
        browseAlso = kwargs.pop('browseAlso',False)
        params_dict.update(kwargs)

        if print_params:
            print "\nrandom_forest parameters:", params_dict

        a = self.__check_request(requests.get(
            url=self.__url('RF.json'),
            timeout=timeoutSecs,
            params=params_dict))
        verboseprint("\nrandom_forest result:", dump_json(a))
        return a

    def random_forest_view(self, data_key, model_key, timeoutSecs=300, print_params=False, **kwargs):
        # UPDATE: only pass the minimal set of params to RFView. It should get the 
        # rest from the model. what about classWt? It can be different between RF and RFView?
        params_dict = {
            'data_key': data_key,
            'model_key': model_key,
            'out_of_bag_error_estimate': 1, 
            'class_weights': None,
            'response_variable': None, # FIX! apparently this is needed now?
            }
        browseAlso = kwargs.pop('browseAlso',False)

        # only update params_dict..don't add
        # throw away anything else as it should come from the model (propagating what RF used)
        for k in kwargs:
            if k in params_dict:
                params_dict[k] = kwargs[k]

        if print_params:
            print "\nrandom_forest_view parameters:", params_dict

        a = self.__check_request(requests.get(
            self.__url('RFView.json'),
            timeout=timeoutSecs,
            params=params_dict))
        verboseprint("\nrandom_forest_view result:", dump_json(a))

        if (browseAlso | browse_json):
            h2b.browseJsonHistoryAsUrlLastMatch("RFView")
        return a

    def random_forest_treeview(self, tree_number, data_key, model_key, 
        timeoutSecs=10, ignoreH2oError=False, **kwargs):
        params_dict = {
            'tree_number': tree_number,
            'data_key': data_key,
            'model_key': model_key,
            }

        browseAlso = kwargs.pop('browseAlso',False)
        params_dict.update(kwargs)

        a = self.__check_request(requests.get(
                self.__url('RFTreeView.json'),
                timeout=timeoutSecs,
                params=params_dict),
            ignoreH2oError=ignoreH2oError)

        verboseprint("\nrandom_forest_treeview result:", dump_json(a))
        # Always do it to eyeball?
        if (browseAlso | browse_json | True):
            h2b.browseJsonHistoryAsUrlLastMatch("RFTreeView")
            time.sleep(3) # to be able to see it
        return a

    # kwargs used to pass many params
    def GLM_shared(self, key, 
        timeoutSecs=300, retryDelaySecs=0.5, initialDelaySecs=None, pollTimeoutSecs=30,
        parentName=None, **kwargs):

        browseAlso = kwargs.pop('browseAlso',False)
        params_dict = { 
            'family': 'binomial',
            'key': key,
            'y': 1,
            'link': 'familyDefault'
        }
        params_dict.update(kwargs)
        print "\nGLM params list", params_dict

        a = self.__check_request(requests.get(
            self.__url(parentName + '.json'),
            timeout=timeoutSecs,
            params=params_dict))
        
        verboseprint(parentName, dump_json(a))
        return a 

    def GLM(self, key, 
        timeoutSecs=300, retryDelaySecs=0.5, initialDelaySecs=None, pollTimeoutSecs=30, 
        noise=None, noPoll=False, **kwargs):

        a = self.GLM_shared(key, timeoutSecs, retryDelaySecs, initialDelaySecs, parentName="GLM", **kwargs)
        # Check that the response has the right Progress url it's going to steer us to.
        if a['response']['redirect_request']!='GLMProgressPage':
            print dump_json(a)
            raise Exception('H2O GLM redirect is not GLMProgressPage. GLM json response precedes.')

        if noPoll:
            return a

        a = self.poll_url(a['response'],
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, 
            initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs, noise=noise)
        verboseprint("GLM done:", dump_json(a))

        browseAlso = kwargs.get('browseAlso', False)
        if (browseAlso | browse_json):
            print "Viewing the GLM result through the browser"
            h2b.browseJsonHistoryAsUrlLastMatch('GLMProgressPage')
            time.sleep(5)
        return a

    # this only exists in new. old will fail
    def GLMGrid(self, key, 
        timeoutSecs=300, retryDelaySecs=1.0, initialDelaySecs=None, pollTimeoutSecs=30,
        noise=None, noPoll=False, **kwargs):

        a = self.GLM_shared(key, timeoutSecs, retryDelaySecs, initialDelaySecs, parentName="GLMGrid", **kwargs)
        # Check that the response has the right Progress url it's going to steer us to.
        if a['response']['redirect_request']!='GLMGridProgress':
            print dump_json(a)
            raise Exception('H2O GLMGrid redirect is not GLMGridProgress. GLMGrid json response precedes.')

        if noPoll:
            return a

        a = self.poll_url(a['response'],
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, 
            initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs, noise=noise)
        verboseprint("GLMGrid done:", dump_json(a))

        browseAlso = kwargs.get('browseAlso', False)
        if (browseAlso | browse_json):
            print "Viewing the GLM grid result through the browser"
            h2b.browseJsonHistoryAsUrlLastMatch('GLMGridProgress')
            time.sleep(5)
        return a


    # GLMScore params
    # model_key=__GLMModel_7a3a73c1-f272-4a2e-b37f-d2f371d304ba&
    # key=cuse.hex&
    # thresholds=0%3A1%3A0.01
    def GLMScore(self, key, model_key, timeoutSecs=100, **kwargs):
        browseAlso = kwargs.pop('browseAlso',False)
        # i guess key and model_key could be in kwargs, but 
        # maybe separate is more consistent with the core key behavior
        # elsewhere
        params_dict = { 
            'key': key,
            'model_key': model_key,
        }
        params_dict.update(kwargs)
        print "\nGLMScore params list", params_dict

        a = self.__check_request(requests.get(
            self.__url('GLMScore.json'),
            timeout=timeoutSecs,
            params=params_dict))
        verboseprint("GLMScore:", dump_json(a))

        browseAlso = kwargs.get('browseAlso', False)
        if (browseAlso | browse_json):
            print "Redoing the GLMScore through the browser, no results saved though"
            h2b.browseJsonHistoryAsUrlLastMatch('GLMScore')
            time.sleep(5)
        return a 

    def stabilize(self, test_func, error, timeoutSecs=10, retryDelaySecs=0.5):
        '''Repeatedly test a function waiting for it to return True.

        Arguments:
        test_func      -- A function that will be run repeatedly
        error          -- A function that will be run to produce an error message
                          it will be called with (node, timeTakenSecs, numberOfRetries)
                    OR
                       -- A string that will be interpolated with a dictionary of
                          { 'timeTakenSecs', 'numberOfRetries' }
        timeoutSecs    -- How long in seconds to keep trying before declaring a failure
        retryDelaySecs -- How long to wait between retry attempts
        '''
        start = time.time()
        numberOfRetries = 0
        while time.time() - start < timeoutSecs:
            if test_func(self, tries=numberOfRetries):
                break
            time.sleep(retryDelaySecs)
            numberOfRetries += 1
            # hey, check the sandbox if we've been waiting a long time...rather than wait for timeout
            # to find the badness?. can check_sandbox_for_errors at any time 
            if ((numberOfRetries%50)==0):
                check_sandbox_for_errors()

        else:
            timeTakenSecs = time.time() - start
            if isinstance(error, type('')):
                raise Exception('%s failed after %.2f seconds having retried %d times' % (
                            error, timeTakenSecs, numberOfRetries))
            else:
                msg = error(self, timeTakenSecs, numberOfRetries)
                raise Exception(msg)

    def wait_for_node_to_accept_connections(self,timeoutSecs=15):
        verboseprint("wait_for_node_to_accept_connections")
        def test(n, tries=None):
            try:
                n.get_cloud()
                return True
            except requests.ConnectionError, e:
                # Now using: requests 1.1.0 (easy_install --upgrade requests) 2/5/13
                # Now: assume all requests.ConnectionErrors are H2O legal connection errors.
                # Have trouble finding where the errno is, fine to assume all are good ones.
                # Timeout check will kick in if continued H2O badness.
                return False

        self.stabilize(test, 'Cloud accepting connections',
                timeoutSecs=timeoutSecs, # with cold cache's this can be quite slow
                retryDelaySecs=0.1) # but normally it is very fast

    def sandbox_error_report(self, done=None):
        # not clearable..just or in new value
        if done:
            self.sandbox_error_was_reported = True
        return (self.sandbox_error_was_reported)

    def get_args(self):
        #! FIX! is this used for both local and remote? 
        # I guess it doesn't matter if we use flatfile for both now
        args = [ 'java' ]

        # defaults to not specifying
	# FIX! we need to check that it's not outside the limits of the dram of the machine it's running on?
        if self.java_heap_GB is not None:
            if (1 > self.java_heap_GB > 63):
                raise Exception('java_heap_GB <1 or >63  (GB): %s' % (self.java_heap_GB))
            args += [ '-Xms%dG' % self.java_heap_GB ]
            args += [ '-Xmx%dG' % self.java_heap_GB ]

        if self.java_heap_MB is not None:
            if (1 > self.java_heap_MB > 63000):
                raise Exception('java_heap_MB <1 or >63000  (MB): %s' % (self.java_heap_MB))
            args += [ '-Xms%dm' % self.java_heap_MB ]
            args += [ '-Xmx%dm' % self.java_heap_MB ]

        if self.java_extra_args is not None:
            args += [ '%s' % self.java_extra_args ]

        if self.use_debugger:
            args += ['-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000']
        args += ["-ea"]
        if self.classpath:
            entries = [ find_file('build/classes'), find_file('lib/javassist.jar') ] 
            entries += glob.glob(find_file('lib')+'/*/*.jar')
            entries += glob.glob(find_file('lib')+'/*/*/*.jar')
            args += ['-classpath', os.pathsep.join(entries), 'water.Boot']
        else: 
            args += ["-jar", self.get_h2o_jar()]

        # H2O should figure it out, if not specified
        if self.addr is not None:
            args += [
                '--ip=%s' % self.addr,
                ]

        # Need to specify port, since there can be multiple ports for an ip in the flatfile
        if self.port is not None:
            args += [
                "--port=%d" % self.port,
            ]

        if self.use_flatfile:
            args += [
                '--flatfile=' + self.flatfile,
            ]

        args += [
            '--ice_root=%s' % self.get_ice_dir(),
            # if I have multiple jenkins projects doing different h2o clouds, I need
            # I need different ports and different cloud name.
            # does different cloud name prevent them from joining up 
            # (even if same multicast ports?)
            # I suppose I can force a base address. or run on another machine?
            '--name=' + cloud_name()
            ]

        # FIX! need to be able to specify name node/path for non-0xdata hdfs
        # specifying hdfs stuff when not used shouldn't hurt anything
        if self.use_hdfs:
            # NOTE: was leaving space. should work, but try =
            args += [
                # it's fine if hdfs_name has a ":9000" port or something too
                '-hdfs hdfs://' + self.hdfs_name_node,
                '-hdfs_version=' + self.hdfs_version, 
            ]
        if self.hdfs_config:
            args += [
                '-hdfs_config ' + self.hdfs_config
            ]

        if not self.sigar:
            args += ['--nosigar']

        if self.aws_credentials:
            args += [ '--aws_credentials='+self.aws_credentials ]

        # passed thru build_cloud in test, or global from commandline arg
        if self.random_udp_drop or random_udp_drop:
            args += ['--random_udp_drop']

        if self.disable_h2o_log:
            args += ['--nolog']

        return args

    def __init__(self, 
        use_this_ip_addr=None, port=54321, capture_output=True, sigar=False, 
        use_debugger=None, classpath=None,
        use_hdfs=False, 
        # hdfs_version="cdh4", hdfs_name_node="192.168.1.151", 
        hdfs_version="cdh3u5", hdfs_name_node="192.168.1.176", 
        hdfs_config=None,
        aws_credentials=None,
        use_flatfile=False, java_heap_GB=None, java_heap_MB=None, java_extra_args=None, 
        use_home_for_ice=False, node_id=None, username=None,
        random_udp_drop=False,
        redirect_import_folder_to_s3_path=None,
        disable_h2o_log=False, 
        enable_benchmark_log=False,
        ):
 
        self.redirect_import_folder_to_s3_path = redirect_import_folder_to_s3_path

        if use_debugger is None: use_debugger = debugger
        self.aws_credentials = aws_credentials
        self.port = port
        # None is legal for self.addr. means we won't give an ip to the jar when we start, and it should
        # figure out the right thing. Or we can say use use_this_ip_addr=127.0.0.1, or the known address 
        # if use_this_addr is None, use 127.0.0.1 for urls and json
        # Command line arg 'ipaddr' dominates:
        if ipaddr:
            self.addr = ipaddr
        else:
            self.addr = use_this_ip_addr

        if use_this_ip_addr is not None:
            self.http_addr = use_this_ip_addr
        else:
            self.http_addr = get_ip_address()

        self.sigar = sigar
        self.use_debugger = use_debugger
        self.classpath = classpath
        self.capture_output = capture_output

        self.use_hdfs = use_hdfs
        self.hdfs_name_node = hdfs_name_node
        self.hdfs_version = hdfs_version
        self.hdfs_config = hdfs_config

        self.use_flatfile = use_flatfile
        self.java_heap_GB = java_heap_GB
        self.java_heap_MB = java_heap_MB
        self.java_extra_args = java_extra_args

        self.use_home_for_ice = use_home_for_ice
        self.node_id = node_id
        self.username = username

        # don't want multiple reports from tearDown and tearDownClass
        # have nodes[0] remember (0 always exists)
        self.sandbox_error_was_reported = False
        self.sandbox_ignore_errors = False

        self.random_udp_drop = random_udp_drop
        self.disable_h2o_log = disable_h2o_log

        # this dumps stats from tests, and perf stats while polling to benchmark.log
        self.enable_benchmark_log = enable_benchmark_log


    def __str__(self):
        return '%s - http://%s:%d/' % (type(self), self.http_addr, self.port)

    def get_ice_dir(self):
        raise Exception('%s must implement %s' % (type(self), inspect.stack()[0][3]))

    def get_h2o_jar(self):
        raise Exception('%s must implement %s' % (type(self), inspect.stack()[0][3]))

    def is_alive(self):
        raise Exception('%s must implement %s' % (type(self), inspect.stack()[0][3]))

    def terminate(self):
        raise Exception('%s must implement %s' % (type(self), inspect.stack()[0][3]))

class ExternalH2O(H2O):
    '''An H2O instance launched outside the control of python'''
    def __init__(self, *args, **kwargs):
        super(ExternalH2O, self).__init__(*args, **kwargs)

    def get_h2o_jar(self):
        return find_file('target/h2o.jar') # just a likely guess

    def get_ice_dir(self):
        return '/tmp/ice%d' % self.port # just a likely guess

    def is_alive(self):
        try:
            self.get_cloud()
            return True
        except:
            return False

    def terminate(self):
        # try/except for this is inside shutdown_all now
        self.shutdown_all()
        if self.is_alive():
            raise Exception('Unable to terminate externally launched node: %s' % self)


class LocalH2O(H2O):
    '''An H2O instance launched by the python framework on the local host using psutil'''
    def __init__(self, *args, **kwargs):
        super(LocalH2O, self).__init__(*args, **kwargs)
        self.rc = None
        # FIX! no option for local /home/username ..always the sandbox (LOG_DIR)
        self.ice = tmp_dir('ice.')
        self.flatfile = flatfile_name()
        if self.node_id is not None:
            logPrefix = 'local-h2o-' + str(self.node_id)
        else:
            logPrefix = 'local-h2o'
        check_port_group(self.port)
        spawn = spawn_cmd(logPrefix, self.get_args(), capture_output=self.capture_output)
        self.ps = spawn[0]

    def get_h2o_jar(self):
        return find_file('target/h2o.jar')

    def get_flatfile(self):
        return self.flatfile
        # return find_file(flatfile_name())

    def get_ice_dir(self):
        return self.ice

    def is_alive(self):
        verboseprint("Doing is_alive check for LocalH2O", self.wait(0))
        return self.wait(0) is None
    
    def terminate(self):
        # send a shutdown request first. This matches ExternalH2O
        # since local is used for a lot of buggy new code, also do the ps kill.
        # try/except inside shutdown_all now
        self.shutdown_all()

        # we need a delay after shutdown_all above, before this check?
        time.sleep(1)
        if self.is_alive():
            print "\nShutdown didn't work for local node? : %s. Will kill though" % self

        try:
            if self.is_alive(): self.ps.kill()
            if self.is_alive(): self.ps.terminate()
            return self.wait(0.5)
        except psutil.NoSuchProcess:
            return -1

    def wait(self, timeout=0):
        if self.rc is not None: return self.rc
        try:
            self.rc = self.ps.wait(timeout)
            return self.rc
        except psutil.TimeoutExpired:
            return None

    def stack_dump(self):
        self.ps.send_signal(signal.SIGQUIT)

class RemoteHost(object):
    def upload_file(self, f, progress=None):
        # FIX! we won't find it here if it's hdfs://192.168.1.151/ file
        f = find_file(f)
        if f not in self.uploaded:
            import md5
            m = md5.new()
            m.update(open(f).read())
            m.update(getpass.getuser())
            dest = '/tmp/' +m.hexdigest() +"-"+ os.path.basename(f)

            # sigh. we rm/create sandbox in build_cloud now 
            # (because nosetests doesn't exec h2o_main and we 
            # don't want to code "clean_sandbox()" in all the tests.
            # So: we don't have a sandbox here, or if we do, we're going to delete it.
            # Just don't log anything until build_cloud()? that should be okay?
            # we were just logging this upload message..not needed.
            # log('Uploading to %s: %s -> %s' % (self.http_addr, f, dest))
            sftp = self.ssh.open_sftp()
            # check if file exists on remote side
            try:
                sftp.stat(dest)
                print "Skipping upload of file {0}. File {1} exists on remote side!".format(f, dest)
            except IOError, e:
                if e.errno == errno.ENOENT:
                    sftp.put(f, dest, callback=progress)
            finally:
                sftp.close()
            self.uploaded[f] = dest
        sys.stdout.flush()
        return self.uploaded[f]

    def record_file(self, f, dest):
        '''Record a file as having been uploaded by external means'''
        self.uploaded[f] = dest

    def run_cmd(self, cmd):
        log('Running `%s` on %s' % (cmd, self))
        (stdin, stdout, stderr) = self.ssh.exec_command(cmd)
        stdin.close()

        sys.stdout.write(stdout.read())
        sys.stdout.flush()
        stdout.close()

        sys.stderr.write(stderr.read())
        sys.stderr.flush()
        stderr.close()

    def push_file_to_remotes(self, f, hosts):
        dest = self.uploaded[f]
        for h in hosts:
            if h == self: continue
            self.run_cmd('scp %s %s@%s:%s' % (dest, h.username, h.addr, dest))
            h.record_file(f, dest)

    def __init__(self, addr, username, password=None, **kwargs):
        import paramiko
        # To debug paramiko you can use the following code:
        #paramiko.util.log_to_file('/tmp/paramiko.log')
        #paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)
        self.addr = addr
        self.http_addr = addr
        self.username = username
        self.ssh = paramiko.SSHClient()

        # don't require keys. If no password, assume passwordless setup was done
        policy = paramiko.AutoAddPolicy()
        self.ssh.set_missing_host_key_policy(policy)
        self.ssh.load_system_host_keys()
        if password is None:
            self.ssh.connect(self.addr, username=username, **kwargs)
        else:
            self.ssh.connect(self.addr, username=username, password=password, **kwargs)

        # keep connection - send keepalive packet evety 5minutes
        self.ssh.get_transport().set_keepalive(300)
        self.uploaded = {}

    def remote_h2o(self, *args, **kwargs):
        return RemoteH2O(self, self.addr, *args, **kwargs)

    def open_channel(self):
        ch = self.ssh.get_transport().open_session()
        ch.get_pty() # force the process to die without the connection
        return ch

    def __str__(self):
        return 'ssh://%s@%s' % (self.username, self.addr)


class RemoteH2O(H2O):
    '''An H2O instance launched by the python framework on a specified host using openssh'''
    def __init__(self, host, *args, **kwargs):
        super(RemoteH2O, self).__init__(*args, **kwargs)

        self.jar = host.upload_file('target/h2o.jar')
        # need to copy the flatfile. We don't always use it (depends on h2o args)
        self.flatfile = host.upload_file(flatfile_name())
        # distribute AWS credentials
        if self.aws_credentials:
            self.aws_credentials = host.upload_file(self.aws_credentials)

        if self.hdfs_config:
            self.hdfs_config = host.upload_file(self.hdfs_config)

        if self.use_home_for_ice:
            # this will be the username used to ssh to the host
            self.ice = "/home/" + host.username + '/ice.%d.%s' % (self.port, time.time())
        else:
            self.ice = '/tmp/ice.%d.%s' % (self.port, time.time())

        self.channel = host.open_channel()
        ### FIX! TODO...we don't check on remote hosts yet
       
        # this fires up h2o over there
        cmd = ' '.join(self.get_args())
        # UPDATE: somehow java -jar on cygwin target (xp) can't handle /tmp/h2o*jar
        # because it's a windows executable and expects windows style path names.
        # but if we cd into /tmp, it can do java -jar h2o*jar.
        # So just split out the /tmp (pretend we don't know) and the h2o jar file name
        # Newer windows may not have this problem? Do the ls (this goes into the local stdout
        # files) so we can see the file is really where we expect.
        # This hack only works when the dest is /tmp/h2o*jar. It's okay to execute
        # with pwd = /tmp. If /tmp/ isn't in the jar path, I guess things will be the same as
        # normal.
        self.channel.exec_command("cd /tmp; ls -ltr "+self.jar+"; "+ \
            re.sub("/tmp/","",cmd)) # removing the /tmp/ we know is in there

        if self.capture_output:
            if self.node_id is not None:
                logPrefix = 'remote-h2o-' + str(self.node_id)
            else:
                logPrefix = 'remote-h2o'

            outfd,outpath = tmp_file(logPrefix + '.stdout.', '.log')
            errfd,errpath = tmp_file(logPrefix + '.stderr.', '.log')

            drain(self.channel.makefile(), outfd)
            drain(self.channel.makefile_stderr(), errfd)
            comment = 'Remote on %s, stdout %s, stderr %s' % (
                self.addr, os.path.basename(outpath), os.path.basename(errpath))
        else:
            drain(self.channel.makefile(), sys.stdout)
            drain(self.channel.makefile_stderr(), sys.stderr)
            comment = 'Remote on %s' % self.addr

        log(cmd, comment=comment)

    def get_h2o_jar(self):
        return self.jar

    def get_flatfile(self):
        return self.flatfile

    def get_ice_dir(self):
        return self.ice

    def is_alive(self):
        verboseprint("Doing is_alive check for RemoteH2O")
        if self.channel.closed: return False
        if self.channel.exit_status_ready(): return False
        try:
            self.get_cloud()
            return True
        except:
            return False

    def terminate(self):
        self.shutdown_all()
        self.channel.close()
        # kbn: it should be dead now? want to make sure we don't have zombies
        # we should get a connection error. doing a is_alive subset.
        try:
            gc_output = self.get_cloud()
            raise Exception("get_cloud() should fail after we terminate a node. It isn't. %s %s" % (self, gc_output))
        except:
            return True
    
