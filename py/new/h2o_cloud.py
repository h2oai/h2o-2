import time, os, stat, json, signal, tempfile, shutil, datetime, inspect, threading, getpass
import requests, argparse, sys, unittest, glob
import urlparse, logging, random
import psutil, requests
import h2o_sandbox
# used in shutil.rmtree permission hack for windows
import errno

# For checking ports in use, using netstat thru a subprocess.
from subprocess import Popen, PIPE


def verboseprint(*args, **kwargs):
    if verbose:
        for x in args: # so you don't have to create a single string
            print x,
        for x in kwargs: # so you don't have to create a single string
            print x,
        print
        # so we can see problems when hung?
        sys.stdout.flush()

# The cloud is uniquely named per user (only)
# Fine to uniquely identify the flatfile by name only also?
# Both are the user that runs the test. The config might have a different username on the
# remote machine (0xdiag, say, or hduser)
def flatfile_name():
    return ('pytest_flatfile-%s' % getpass.getuser())

# only usable after you've built a cloud (junit, watch out)
def cloud_name():
    return nodes[0].cloud_name

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
    t = threading.Thread(target=__drain, args=(src, dst))
    t.daemon = True
    t.start()

# Hackery: find the ip address that gets you to Google's DNS
# Trickiness because you might have multiple IP addresses (Virtualbox), or Windows.
# we used to not like giving ip 127.0.0.1 to h2o?
def get_ip_address():
    if ipaddr_from_cmd_line:
        verboseprint("get_ip case 1:", ipaddr_from_cmd_line)
        return ipaddr_from_cmd_line

    import socket

    ip = '127.0.0.1'
    socket.setdefaulttimeout(0.5)
    hostname = socket.gethostname()
    # this method doesn't work if vpn is enabled..it gets the vpn ip
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 0))
        ip = s.getsockname()[0]
        verboseprint("get_ip case 2:", ip)
    except:
        pass

    if ip.startswith('127'):
        # drills down into family
        ip = socket.getaddrinfo(hostname, None)[0][4][0]
        verboseprint("get_ip case 3:", ip)

    ipa = None
    try:
        # Translate a host name to IPv4 address format, extended interface. 
        # Return a triple (hostname, aliaslist, ipaddrlist) 
        # where hostname is the primary host name responding to the given ip_address, 
        # aliaslist is a (possibly empty) list of alternative host names for the same address, and 
        # ipaddrlist is a list of IPv4 addresses for the same interface on the same host
        ghbx = socket.gethostbyname_ex(hostname)
        for ips in ghbx[2]:
            # only take the first
            if ipa is None and not ips.startswith("127."):
                ipa = ips[:]
                verboseprint("get_ip case 4:", ipa)
                if ip != ipa:
                    print "\nAssuming", ip, "is the ip address h2o will use but", ipa, "is probably the real ip?"
                    print "You might have a vpn active. Best to use '-ip " + ipa + "' to get python and h2o the same."
    except:
        pass
        # print "Timeout during socket.gethostbyname_ex(hostname)"

    verboseprint("get_ip_address:", ip)
    # set it back to default higher timeout (None would be no timeout?)
    socket.setdefaulttimeout(5)
    return ip


def get_sandbox_name():
    if os.environ.has_key("H2O_SANDBOX_NAME"):
        return os.environ["H2O_SANDBOX_NAME"]
    else:
        return "sandbox"


def unit_main():
    global python_test_name, python_cmd_args, python_cmd_line, python_cmd_ip, python_username
    # python_test_name = inspect.stack()[1][1]
    python_test_name = ""
    python_cmd_args = " ".join(sys.argv[1:])
    python_cmd_line = "python %s %s" % (python_test_name, python_cmd_args)
    python_username = getpass.getuser()
    print "\nTest: %s    command line: %s" % (python_test_name, python_cmd_line)

    parse_our_args()
    unittest.main()

verbose = False
ipaddr_from_cmd_line = None
config_json = None
debugger = False
random_seed = None
beta_features = True
abort_after_import = False
debug_rest = False
# jenkins gets this assign, but not the unit_main one?
# python_test_name = inspect.stack()[1][1]
python_test_name = ""

# trust what the user says!
if ipaddr_from_cmd_line:
    python_cmd_ip = ipaddr_from_cmd_line
else:
    python_cmd_ip = get_ip_address()

# no command line args if run with just nose
python_cmd_args = ""
# don't really know what it is if nosetests did some stuff. Should be just the test with no args
python_cmd_line = ""
python_username = getpass.getuser()


def parse_our_args():
    parser = argparse.ArgumentParser()
    # can add more here
    parser.add_argument('-v', '--verbose', help='increased output', action='store_true')
    parser.add_argument('-ip', '--ip', type=str, help='IP address to use for single host H2O with psutil control')
    parser.add_argument('-cj', '--config_json',
                        help='Use this json format file to provide multi-host defaults. Overrides the default file pytest_config-<username>.json. These are used only if you do build_cloud_with_hosts()')
    parser.add_argument('-dbg', '--debugger', help='Launch java processes with java debug attach mechanisms',
                        action='store_true')
    parser.add_argument('-s', '--random_seed', type=int, help='initialize SEED (64-bit integer) for random generators')
    parser.add_argument('-bf', '--beta_features', help='enable or switch to beta features (import2/parse2)',
                        action='store_true')
    parser.add_argument('-debug_rest', '--debug_rest', help='Print REST API interactions to rest.log',
                        action='store_true')

    parser.add_argument('unittest_args', nargs='*')
    args = parser.parse_args()

    global verbose, ipaddr_from_cmd_line, config_json, debugger
    global random_seed, beta_features, debug_rest

    verbose = args.verbose
    ipaddr_from_cmd_line = args.ip
    config_json = args.config_json
    debugger = args.debugger
    random_seed = args.random_seed
    debug_rest = args.debug_rest

    # Set sys.argv to the unittest args (leav sys.argv[0] as is)
    # FIX! this isn't working to grab the args we don't care about
    # Pass "--failfast" to stop on first error to unittest. and -v
    # won't get this for jenkins, since it doesn't do parse_our_args
    sys.argv[1:] = ['-v', "--failfast"] + args.unittest_args
    # sys.argv[1:] = args.unittest_args


def find_file(base):
    f = base
    if not os.path.exists(f): f = '../' + base
    if not os.path.exists(f): f = '../../' + base
    if not os.path.exists(f): f = 'py/' + base
    # these 2 are for finding from h2o-perf
    if not os.path.exists(f): f = '../h2o/' + base
    if not os.path.exists(f): f = '../../h2o/' + base
    if not os.path.exists(f):
        raise Exception("unable to find file %s" % base)
    return f

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

LOG_DIR = get_sandbox_name()

def clean_sandbox():
    if os.path.exists(LOG_DIR):

        # shutil.rmtree hangs if symlinks in the dir? (in syn_datasets for multifile parse)
        # use os.remove() first
        for f in glob.glob(LOG_DIR + '/syn_datasets/*'):
            verboseprint("cleaning", f)
            os.remove(f)

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


def tmp_file(prefix='', suffix='', tmp_dir=None):
    if not tmp_dir:
        tmpdir = LOG_DIR
    else:
        tmpdir = tmp_dir

    fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=tmpdir)
    # make sure the file now exists
    # os.open(path, 'a').close()
    # give everyone permission to read it (jenkins running as 
    # 0xcustomer needs to archive as jenkins
    permissions = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH
    os.chmod(path, permissions)
    return (fd, path)


def tmp_dir(prefix='', suffix=''):
    return tempfile.mkdtemp(prefix=prefix, suffix=suffix, dir=LOG_DIR)


def log(cmd, comment=None):
    filename = LOG_DIR + '/commands.log'
    # everyone can read
    with open(filename, 'a') as f:
        f.write(str(datetime.datetime.now()) + ' -- ')
        # what got sent to h2o
        # f.write(cmd)
        # let's try saving the unencoded url instead..human readable
        if cmd:
            f.write(urlparse.unquote(cmd))
            if comment:
                f.write('    #')
                f.write(comment)
            f.write("\n")
        elif comment: # for comment-only
            f.write(comment + "\n")
            # jenkins runs as 0xcustomer, and the file wants to be archived by jenkins who isn't in his group
    permissions = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH
    os.chmod(filename, permissions)


def make_syn_dir():
    # move under sandbox
    # the LOG_DIR must have been created for commands.log before any datasets would be created
    SYNDATASETS_DIR = LOG_DIR + '/syn_datasets'
    if os.path.exists(SYNDATASETS_DIR):
        shutil.rmtree(SYNDATASETS_DIR)
    os.mkdir(SYNDATASETS_DIR)
    return SYNDATASETS_DIR


def dump_json(j):
    return json.dumps(j, sort_keys=True, indent=2)

# can't have a list of cmds, because cmd is a list
# cmdBefore gets executed first, and we wait for it to complete
def spawn_cmd(name, cmd, capture_output=True, **kwargs):
    if capture_output:
        outfd, outpath = tmp_file(name + '.stdout.', '.log')
        errfd, errpath = tmp_file(name + '.stderr.', '.log')
        # everyone can read
        ps = psutil.Popen(cmd, stdin=None, stdout=outfd, stderr=errfd, **kwargs)
    else:
        outpath = '<stdout>'
        errpath = '<stderr>'
        ps = psutil.Popen(cmd, **kwargs)

    comment = 'PID %d, stdout %s, stderr %s' % (
        ps.pid, os.path.basename(outpath), os.path.basename(errpath))
    log(' '.join(cmd), comment=comment)
    return (ps, outpath, errpath)


def spawn_wait(ps, stdout, stderr, capture_output=True, timeout=None):
    rc = ps.wait(timeout)
    if capture_output:
        out = file(stdout).read()
        err = file(stderr).read()
    else:
        out = 'stdout not captured'
        err = 'stderr not captured'

    if rc is None:
        ps.terminate()
        raise Exception("%s %s timed out after %d\nstdout:\n%s\n\nstderr:\n%s" %
                        (ps.name, ps.cmdline, timeout or 0, out, err))
    elif rc != 0:
        raise Exception("%s %s failed.\nstdout:\n%s\n\nstderr:\n%s" %
                        (ps.name, ps.cmdline, out, err))
    return rc


def spawn_cmd_and_wait(name, cmd, capture_output=True, timeout=None, **kwargs):
    (ps, stdout, stderr) = spawn_cmd(name, cmd, capture_output, **kwargs)
    spawn_wait(ps, stdout, stderr, capture_output, timeout)

global nodes
nodes = []

# I suppose we could shuffle the flatfile order!
# but it uses hosts, so if that got shuffled, we got it covered?
# the i in xrange part is not shuffled. maybe create the list first, for possible random shuffle
# FIX! default to random_shuffle for now..then switch to not.
def write_flatfile(node_count=2, base_port=54321, hosts=None):
    # always create the flatfile.
    ports_per_node = 2
    pff = open(flatfile_name(), "w+")
    # doing this list outside the loops so we can shuffle for better test variation
    hostPortList = []
    if hosts is None:
        ip = python_cmd_ip
        for i in range(node_count):
            hostPortList.append(ip + ":" + str(base_port + ports_per_node * i))
    else:
        for h in hosts:
            for i in range(node_count):
                # removed leading "/"
                hostPortList.append(h.addr + ":" + str(base_port + ports_per_node * i))

    for hp in hostPortList:
        pff.write(hp + "\n")
    pff.close()


def check_h2o_version():
    # assumes you want to know about 3 ports starting at base_port
    command1Split = ['java', '-jar', find_file('target/h2o.jar'), '--version']
    command2Split = ['egrep', '-v', '( Java | started)']
    print "Running h2o to get java version"
    p1 = Popen(command1Split, stdout=PIPE)
    p2 = Popen(command2Split, stdin=p1.stdout, stdout=PIPE)
    output = p2.communicate()[0]
    print output


def default_hosts_file():
    if os.environ.has_key("H2O_HOSTS_FILE"):
        return os.environ["H2O_HOSTS_FILE"]
    return 'pytest_config-{0}.json'.format(getpass.getuser())

# node_count is per host if hosts is specified.
def build_cloud(node_count=1, base_port=54321, hosts=None,
                timeoutSecs=30, retryDelaySecs=1, cleanup=True, 
                conservative=False, **kwargs):
    clean_sandbox()
    log("#*********************************************************************")
    log("Starting new test: " + python_test_name + " at build_cloud()")
    log("#*********************************************************************")

    # start up h2o to report the java version (once). output to python stdout
    # only do this for regression testing
    if getpass.getuser() == 'jenkins':
        check_h2o_version()

    # keep this param in kwargs, because we pass it to the H2O node build, so state
    # is created that polling and other normal things can check, to decide to dump
    # info to benchmark.log
    if kwargs.setdefault('enable_benchmark_log', False):
        setup_benchmark_log()

    ports_per_node = 2
    nodeList = []
    try:
        # if no hosts list, use psutil method on local host.
        totalNodes = 0
        # doing this list outside the loops so we can shuffle for better test variation
        # this jvm startup shuffle is independent from the flatfile shuffle
        portList = [base_port + ports_per_node * i for i in range(node_count)]
        if hosts is None:
            # if use_flatfile, we should create it,
            # because tests will just call build_cloud with use_flatfile=True
            # best to just create it all the time..may or may not be used
            write_flatfile(node_count=node_count, base_port=base_port)
            hostCount = 1
            for p in portList:
                verboseprint("psutil starting node", i)
                newNode = LocalH2O(port=p, node_id=totalNodes, **kwargs)
                nodeList.append(newNode)
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
                    hostPortList.append((h, port))
            for (h, p) in hostPortList:
                verboseprint('ssh starting node', totalNodes, 'via', h)
                newNode = h.remote_h2o(port=p, node_id=totalNodes, **kwargs)
                nodeList.append(newNode)
                totalNodes += 1

        verboseprint("Attempting Cloud stabilize of", totalNodes, "nodes on", hostCount, "hosts")
        start = time.time()
        # UPDATE: best to stabilize on the last node!
        stabilize_cloud(nodeList[0], len(nodeList),
                        timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, noExtraErrorCheck=True)
        verboseprint(len(nodeList), "Last added node stabilized in ", time.time() - start, " secs")
        verboseprint("Built cloud: %d nodes on %d hosts, in %d s" % (len(nodeList),
                                                                     hostCount, (time.time() - start)))
        print "Built cloud:", nodeList[0].java_heap_GB, "GB java heap(s) with", len(nodeList), "total nodes"

        # FIX! using "consensus" in node[-1] should mean this is unnecessary?
        # maybe there's a bug. For now do this. long term: don't want?
        # UPDATE: do it for all cases now 2/14/13
        if conservative: # still needed?
            for n in nodeList:
                stabilize_cloud(n, len(nodeList), timeoutSecs=timeoutSecs, noExtraErrorCheck=True)

        # this does some extra checking now
        verify_cloud_size(nodeList)

        # best to check for any errors due to cloud building right away?
        check_sandbox_for_errors(python_test_name=python_test_name)

    except:
        if cleanup:
            for n in nodeList: n.terminate()
        else:
            nodes[:] = nodeList
        check_sandbox_for_errors(python_test_name=python_test_name)
        raise

    # this is just in case they don't assign the return to the nodes global?
    nodes[:] = nodeList
    print len(nodeList), "total jvms in H2O cloud"
    # put the test start message in the h2o log, to create a marker
    nodes[0].h2o_log_msg()

    if config_json:
        # like cp -p. Save the config file, to sandbox
        print "Saving the ", config_json, "we used to", LOG_DIR
        shutil.copy(config_json, LOG_DIR + "/" + os.path.basename(config_json))

    # Figure out some stuff about how this test was run
    cs_time = str(datetime.datetime.now())
    cs_cwd = os.getcwd()
    cs_python_cmd_line = "python %s %s" % (python_test_name, python_cmd_args)
    cs_python_test_name = python_test_name
    if config_json:
        cs_config_json = os.path.abspath(config_json)
    else:
        cs_config_json = None
    cs_username = python_username
    cs_ip = python_cmd_ip

    return nodeList


def upload_jar_to_remote_hosts(hosts, slow_connection=False):
    def prog(sofar, total):
        # output is bad for jenkins.
        username = getpass.getuser()
        if username != 'jenkins':
            p = int(10.0 * sofar / total)
            sys.stdout.write('\rUploading jar [%s%s] %02d%%' % ('#' * p, ' ' * (10 - p), 100 * sofar / total))
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


def check_sandbox_for_errors(cloudShutdownIsError=False, sandboxIgnoreErrors=False, python_test_name=''):
    # dont' have both tearDown and tearDownClass report the same found error
    # only need the first
    if nodes and nodes[0].sandbox_error_report(): # gets current state
        return

    # Can build a cloud that ignores all sandbox things that normally fatal the test
    # Kludge, test will set this directly if it wants, rather than thru build_cloud parameter.
    # we need the sandbox_ignore_errors, for the test teardown_cloud..the state disappears!
    ignore = sandboxIgnoreErrors or (nodes and nodes[0].sandbox_ignore_errors)
    errorFound = h2o_sandbox.check_sandbox_for_errors(
        LOG_DIR=LOG_DIR,
        sandboxIgnoreErrors=ignore,
        cloudShutdownIsError=cloudShutdownIsError,
        python_test_name=python_test_name)

    if errorFound and nodes:
        nodes[0].sandbox_error_report(True) # sets


def tear_down_cloud(nodeList=None, sandboxIgnoreErrors=False):
    if not nodeList: nodeList = nodes
    try:
        for n in nodeList:
            n.terminate()
            verboseprint("tear_down_cloud n:", n)
    finally:
        check_sandbox_for_errors(sandboxIgnoreErrors=sandboxIgnoreErrors, python_test_name=python_test_name)
        nodeList[:] = []

# timeoutSecs is per individual node get_cloud()
def verify_cloud_size(nodeList=None, verbose=False, timeoutSecs=10, ignoreHealth=False):
    if not nodeList: nodeList = nodes

    expectedSize = len(nodeList)
    # cloud size and consensus have to reflect a single grab of information from a node.
    cloudStatus = [n.get_cloud(timeoutSecs=timeoutSecs) for n in nodeList]

    cloudSizes = [c['cloud_size'] for c in cloudStatus]
    cloudConsensus = [c['consensus'] for c in cloudStatus]
    cloudHealthy = [c['cloud_healthy'] for c in cloudStatus]

    if not all(cloudHealthy):
        msg = "Some node reported cloud_healthy not true: %s" % cloudHealthy
        if not ignoreHealth:
            raise Exception(msg)

    # gather up all the node_healthy status too
    for i, c in enumerate(cloudStatus):
        nodesHealthy = [n['node_healthy'] for n in c['nodes']]
        if not all(nodesHealthy):
            print "node %s cloud status: %s" % (i, dump_json(c))
            msg = "node %s says some node is not reporting node_healthy: %s" % (c['node_name'], nodesHealthy)
            if not ignoreHealth:
                raise Exception(msg)

    if expectedSize == 0 or len(cloudSizes) == 0 or len(cloudConsensus) == 0:
        print "\nexpectedSize:", expectedSize
        print "cloudSizes:", cloudSizes
        print "cloudConsensus:", cloudConsensus
        raise Exception("Nothing in cloud. Can't verify size")

    for s in cloudSizes:
        consensusStr = (",".join(map(str, cloudConsensus)))
        sizeStr = (",".join(map(str, cloudSizes)))
        if (s != expectedSize):
            raise Exception("Inconsistent cloud size." +
                            "nodeList report size: %s consensus: %s instead of %d." % \
                            (sizeStr, consensusStr, expectedSize))
    return (sizeStr, consensusStr, expectedSize)


def stabilize_cloud(node, node_count, timeoutSecs=14.0, retryDelaySecs=0.25, noExtraErrorCheck=False):
    node.wait_for_node_to_accept_connections(timeoutSecs, noExtraErrorCheck=noExtraErrorCheck)

    # want node saying cloud = expected size, plus thinking everyone agrees with that.
    def test(n, tries=None):
        c = n.get_cloud(noExtraErrorCheck=True)
        # don't want to check everything. But this will check that the keys are returned!
        consensus = c['consensus']
        locked = c['locked']
        cloud_size = c['cloud_size']
        cloud_name = c['cloud_name']
        node_name = c['node_name']

        if 'nodes' not in c:
            emsg = "\nH2O didn't include a list of nodes in get_cloud response after initial cloud build"
            raise Exception(emsg)

        # only print it when you get consensus
        if cloud_size != node_count:
            verboseprint("\nNodes in cloud while building:")
            for ci in c['nodes']:
                verboseprint(ci['name'])

        if (cloud_size > node_count):
            emsg = (
                "\n\nERROR: cloud_size: %d reported via json is bigger than we expect: %d" % (cloud_size, node_count) +
                "\nYou likely have zombie(s) with the same cloud name on the network, that's forming up with you." +
                "\nLook at the cloud IP's in 'grep Paxos sandbox/*stdout*' for some IP's you didn't expect." +
                "\n\nYou probably don't have to do anything, as the cloud shutdown in this test should" +
                "\nhave sent a Shutdown.json to all in that cloud (you'll see a kill -2 in the *stdout*)." +
                "\nIf you try again, and it still fails, go to those IPs and kill the zombie h2o's." +
                "\nIf you think you really have an intermittent cloud build, report it." +
                "\n" +
                "\nUPDATE: building cloud size of 2 with 127.0.0.1 may temporarily report 3 incorrectly, with no zombie?"
            )
            raise Exception(emsg)

        a = (cloud_size == node_count) and consensus
        if a:
            verboseprint("\tLocked won't happen until after keys are written")
            verboseprint("\nNodes in final cloud:")
            for ci in c['nodes']:
                verboseprint(ci['name'])

        return a

    node.stabilize(test, error=('A cloud of size %d' % node_count),
                   timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs)


def log_rest(s):
    if not debug_rest:
        return
    rest_log_file = open(os.path.join(LOG_DIR, "rest.log"), "a")
    rest_log_file.write(s)
    rest_log_file.write("\n")
    rest_log_file.close()


class H2O(object):
    def __url(self, loc, port=None):
        # always use the new api port
        if port is None: port = self.port
        if loc.startswith('/'):
            delim = ''
        else:
            delim = '/'
        u = 'http://%s:%d%s%s' % (self.http_addr, port, delim, loc)
        return u


    def __do_json_request(self, jsonRequest=None, fullUrl=None, timeout=10, params=None, returnFast=False,
                          cmd='get', extraComment=None, ignoreH2oError=False, noExtraErrorCheck=False, **kwargs):
    # if url param is used, use it as full url. otherwise crate from the jsonRequest
        if fullUrl:
            url = fullUrl
        else:
            url = self.__url(jsonRequest)

        # remove any params that are 'None'
        # need to copy dictionary, since can't delete while iterating
        if params is not None:
            params2 = params.copy()
            for k in params2:
                if params2[k] is None:
                    del params[k]
            paramsStr = '?' + '&'.join(['%s=%s' % (k, v) for (k, v) in params.items()])
        else:
            paramsStr = ''

        if extraComment:
            log('Start ' + url + paramsStr, comment=extraComment)
        else:
            log('Start ' + url + paramsStr)

        log_rest("")
        log_rest("----------------------------------------------------------------------\n")
        if extraComment:
            log_rest("# Extra comment info about this request: " + extraComment)
        if cmd == 'get':
            log_rest("GET")
        else:
            log_rest("POST")
        log_rest(url + paramsStr)

        # file get passed thru kwargs here
        try:
            if cmd == 'post':
                r = requests.post(url, timeout=timeout, params=params, **kwargs)
            else:
                r = requests.get(url, timeout=timeout, params=params, **kwargs)

        except Exception, e:
            # rethrow the exception after we've checked for stack trace from h2o
            # out of memory errors maybe don't show up right away? so we should wait for h2o
            # to get it out to h2o stdout. We don't want to rely on cloud teardown to check
            # because there's no delay, and we don't want to delay all cloud teardowns by waiting.
            # (this is new/experimental)
            exc_info = sys.exc_info()
            # use this to ignore the initial connection errors during build cloud when h2o is coming up
            if not noExtraErrorCheck: 
                print "ERROR: got exception on %s to h2o. \nGoing to check sandbox, then rethrow.." % (url + paramsStr)
                time.sleep(2)
                check_sandbox_for_errors(python_test_name=python_test_name);
            log_rest("")
            log_rest("EXCEPTION CAUGHT DOING REQUEST: " + str(e.message))
            raise exc_info[1], None, exc_info[2]

        log_rest("")
        try:
            if r is None:
                log_rest("r is None")
            else:
                log_rest("HTTP status code: " + str(r.status_code))
                if hasattr(r, 'text'):
                    if r.text is None:
                        log_rest("r.text is None")
                    else:
                        log_rest(r.text)
                else:
                    log_rest("r does not have attr text")
        except Exception, e:
            # Paranoid exception catch.  
            # Ignore logging exceptions in the case that the above error checking isn't sufficient.
            pass

        # fatal if no response
        if not r:
            raise Exception("Maybe bad url? no r in __do_json_request in %s:" % inspect.stack()[1][3])

        rjson = None
        if returnFast:
            return
        try:
            rjson = r.json()
        except:
            print dump_json(r.text)
            if not isinstance(r, (list, dict)):
                raise Exception("h2o json responses should always be lists or dicts, see previous for text")

            raise Exception("Could not decode any json from the request.")

        # TODO: we should really only look in the response object.  This check
        # prevents us from having a field called "error" (e.g., for a scoring result).
        for e in ['error', 'Error', 'errors', 'Errors']:
            # error can be null (python None). This happens in exec2
            if e in rjson and rjson[e]:
                print "rjson:", dump_json(rjson)
                emsg = 'rjson %s in %s: %s' % (e, inspect.stack()[1][3], rjson[e])
                if ignoreH2oError:
                    # well, we print it..so not totally ignore. test can look at rjson returned
                    print emsg
                else:
                    print emsg
                    raise Exception(emsg)

        for w in ['warning', 'Warning', 'warnings', 'Warnings']:
            # warning can be null (python None).
            if w in rjson and rjson[w]:
                verboseprint(dump_json(rjson))
                print 'rjson %s in %s: %s' % (w, inspect.stack()[1][3], rjson[w])

        return rjson


    def get_cloud(self, noExtraErrorCheck=False, timeoutSecs=10):
        # hardwire it to allow a 60 second timeout
        a = self.__do_json_request('Cloud.json', noExtraErrorCheck=noExtraErrorCheck, timeout=timeoutSecs)

        consensus = a['consensus']
        locked = a['locked']
        cloud_size = a['cloud_size']
        cloud_name = a['cloud_name']
        node_name = a['node_name']
        node_id = self.node_id
        verboseprint('%s%s %s%s %s%s %s%s' % (
            "\tnode_id: ", node_id,
            "\tcloud_size: ", cloud_size,
            "\tconsensus: ", consensus,
            "\tlocked: ", locked,
        ))
        return a

    def h2o_log_msg(self, message=None):
        if 1 == 0:
            return
        if not message:
            message = "\n"
            message += "\n#***********************"
            message += "\npython_test_name: " + python_test_name
            message += "\n#***********************"
        params = {'message': message}
        self.__do_json_request('2/LogAndEcho', params=params)

    # Shutdown url is like a reset button. Doesn't send a response before it kills stuff
    # safer if random things are wedged, rather than requiring response
    # so request library might retry and get exception. allow that.
    def shutdown_all(self):
        try:
            self.__do_json_request('Shutdown.json', noExtraErrorCheck=True)
        except:
            pass
        time.sleep(1) # a little delay needed?
        return (True)

    def put_value(self, value, key=None, repl=None):
        return self.__do_json_request(
            'PutValue.json',
            params={"value": value, "key": key, "replication_factor": repl},
            extraComment=str(value) + "," + str(key) + "," + str(repl))


    # noise is a 2-tuple ("StoreView", none) for url plus args for doing during poll to create noise
    # so we can create noise with different urls!, and different parms to that url
    # no noise if None
    def poll_url(self, response,
                 timeoutSecs=10, retryDelaySecs=0.5, initialDelaySecs=0, pollTimeoutSecs=180,
                 noise=None, benchmarkLogging=None, noPoll=False, reuseFirstPollUrl=False, noPrint=False):
        ### print "poll_url: pollTimeoutSecs", pollTimeoutSecs
        verboseprint('poll_url input: response:', dump_json(response))
        print "at top of poll_url, timeoutSec: ", timeoutSecs

        def get_redirect_url(response):
            url = None
            params = None
            # StoreView has old style, while beta_features
            if 'response_info' in response: 
                response_info = response['response_info']

                if 'redirect_url' not in response_info:
                    raise Exception("Response during polling must have 'redirect_url'\n%s" % dump_json(response))

                if response_info['status'] != 'done':
                    redirect_url = response_info['redirect_url']
                    if redirect_url:
                        url = self.__url(redirect_url)
                        params = None
                    else:
                        if response_info['status'] != 'done':
                            raise Exception(
                                "'redirect_url' during polling is null but status!='done': \n%s" % dump_json(response))
            else:
                if 'response' not in response:
                    raise Exception("'response' not in response.\n%s" % dump_json(response))

                if response['response']['status'] != 'done':
                    if 'redirect_request' not in response['response']:
                        raise Exception("'redirect_request' not in response. \n%s" % dump_json(response))

                    url = self.__url(response['response']['redirect_request'])
                    params = response['response']['redirect_request_args']

            return (url, params)

        # if we never poll
        msgUsed = None

        if 'response_info' in response: # trigger v2 for GBM always?
            status = response['response_info']['status']
            progress = response.get('progress', "")
        else:
            r = response['response']
            status = r['status']
            progress = r.get('progress', "")

        doFirstPoll = status != 'done'
        (url, params) = get_redirect_url(response)
        # no need to recreate the string for messaging, in the loop..
        if params:
            paramsStr = '&'.join(['%s=%s' % (k, v) for (k, v) in params.items()])
        else:
            paramsStr = ''

        # FIX! don't do JStack noise for tests that ask for it. JStack seems to have problems
        noise_enable = noise and noise != ("JStack", None)
        if noise_enable:
            print "Using noise during poll_url:", noise
            # noise_json should be like "Storeview"
            (noise_json, noiseParams) = noise
            noiseUrl = self.__url(noise_json + ".json")
            if noiseParams is None:
                noiseParamsStr = ""
            else:
                noiseParamsStr = '&'.join(['%s=%s' % (k, v) for (k, v) in noiseParams.items()])

        start = time.time()
        count = 0
        if initialDelaySecs:
            time.sleep(initialDelaySecs)

        # can end with status = 'redirect' or 'done'
        # Update: on DRF2, the first RF redirects to progress. So we should follow that, and follow any redirect to view?
        # so for v2, we'll always follow redirects?
        # For v1, we're not forcing the first status to be 'poll' now..so it could be redirect or done?(NN score? if blocking)

        # Don't follow the Parse redirect to Inspect, because we want parseResult['destination_key'] to be the end.
        # note this doesn't affect polling with Inspect? (since it doesn't redirect ?
        while status == 'poll' or doFirstPoll or (status == 'redirect' and 'Inspect' not in url):
            count += 1
            if ((time.time() - start) > timeoutSecs):
                # show what we're polling with
                emsg = "Exceeded timeoutSecs: %d secs while polling." % timeoutSecs + \
                       "status: %s, url: %s?%s" % (status, urlUsed, paramsUsedStr)
                raise Exception(emsg)

            if benchmarkLogging:
                cloudPerfH2O.get_log_save(benchmarkLogging)

            # every other one?
            create_noise = noise_enable and ((count % 2) == 0)
            if create_noise:
                urlUsed = noiseUrl
                paramsUsed = noiseParams
                paramsUsedStr = noiseParamsStr
                msgUsed = "\nNoise during polling with"
            else:
                urlUsed = url
                paramsUsed = params
                paramsUsedStr = paramsStr
                msgUsed = "\nPolling with"

            print status, progress, urlUsed
            time.sleep(retryDelaySecs)

            response = self.__do_json_request(fullUrl=urlUsed, timeout=pollTimeoutSecs, params=paramsUsed)
            verboseprint(msgUsed, urlUsed, paramsUsedStr, "Response:", dump_json(response))
            # hey, check the sandbox if we've been waiting a long time...rather than wait for timeout
            if ((count % 6) == 0):
                check_sandbox_for_errors(python_test_name=python_test_name)

            if (create_noise):
                # this guarantees the loop is done, so we don't need to worry about
                # a 'return r' being interpreted from a noise response
                status = 'poll'
                progress = ''
            else:
                doFirstPoll = False
                status = response['response_info']['status']
                progress = response.get('progress', "")
                # get the redirect url
                if not reuseFirstPollUrl: # reuse url for all v1 stuff
                    (url, params) = get_redirect_url(response)

                if noPoll:
                    return response

        # won't print if we didn't poll
        if msgUsed:
            verboseprint(msgUsed, urlUsed, paramsUsedStr, "Response:", dump_json(response))
        return response


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
            if ((numberOfRetries % 50) == 0):
                check_sandbox_for_errors(python_test_name=python_test_name)

        else:
            timeTakenSecs = time.time() - start
            if isinstance(error, type('')):
                raise Exception('%s failed after %.2f seconds having retried %d times' % (
                    error, timeTakenSecs, numberOfRetries))
            else:
                msg = error(self, timeTakenSecs, numberOfRetries)
                raise Exception(msg)

    def wait_for_node_to_accept_connections(self, timeoutSecs=15, noExtraErrorCheck=False):
        verboseprint("wait_for_node_to_accept_connections")

        def test(n, tries=None):
            try:
                n.get_cloud(noExtraErrorCheck=noExtraErrorCheck)
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
        args = ['java']

        # I guess it doesn't matter if we use flatfile for both now
        # defaults to not specifying
        # FIX! we need to check that it's not outside the limits of the dram of the machine it's running on?
        if self.java_heap_GB is not None:
            if not (1 <= self.java_heap_GB <= 256):
                raise Exception('java_heap_GB <1 or >256  (GB): %s' % (self.java_heap_GB))
            args += ['-Xms%dG' % self.java_heap_GB]
            args += ['-Xmx%dG' % self.java_heap_GB]

        if self.java_heap_MB is not None:
            if not (1 <= self.java_heap_MB <= 256000):
                raise Exception('java_heap_MB <1 or >256000  (MB): %s' % (self.java_heap_MB))
            args += ['-Xms%dm' % self.java_heap_MB]
            args += ['-Xmx%dm' % self.java_heap_MB]

        if self.java_extra_args is not None:
            args += ['%s' % self.java_extra_args]

        args += ["-ea"]

        if self.use_maprfs:
            args += ["-Djava.library.path=/opt/mapr/lib"]

        if self.classpath:
            entries = [find_file('build/classes'), find_file('lib/javassist.jar')]
            entries += glob.glob(find_file('lib') + '/*/*.jar')
            entries += glob.glob(find_file('lib') + '/*/*/*.jar')
            args += ['-classpath', os.pathsep.join(entries), 'water.Boot']
        else:
            args += ["-jar", self.get_h2o_jar()]

        if 1==1:
            if self.hdfs_config:
                args += [
                    '-hdfs_config=' + self.hdfs_config
                ]

        if beta_features:
            args += ["-beta"]

        # H2O should figure it out, if not specified
        # DON"T EVER USE on multi-machine...h2o should always get it right, to be able to run on hadoop 
        # where it's not told
        if (self.addr is not None) and (not self.remoteH2O):
            args += [
                '--ip=%s' % self.addr,
            ]

        # Need to specify port, since there can be multiple ports for an ip in the flatfile
        if self.port is not None:
            args += [
                "--port=%d" % self.port,
            ]

        if self.use_debugger:
            # currently hardwire the base port for debugger to 8000
            # increment by one for every node we add
            # sence this order is different than h2o cluster order, print out the ip and port for the user
            # we could save debugger_port state per node, but not really necessary (but would be more consistent)
            debuggerBasePort = 8000
            if self.node_id is None:
                debuggerPort = debuggerBasePort
            else:
                debuggerPort = debuggerBasePort + self.node_id

            if self.http_addr:
                a = self.http_addr
            else:
                a = "localhost"

            if self.port:
                b = str(self.port)
            else:
                b = "h2o determined"

            # I guess we always specify port?
            print "You can attach debugger at port %s for jvm at %s:%s" % (debuggerPort, a, b)
            args += ['-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=%s' % debuggerPort]

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
        ]
        args += [
            '--name=' + self.cloud_name
        ]

        # ignore the other -hdfs args if the config is used?
        if 1==0:
            if self.hdfs_config:
                args += [
                    '-hdfs_config=' + self.hdfs_config
                ]

        if self.use_hdfs:
            args += [
                # it's fine if hdfs_name has a ":9000" port or something too
                '-hdfs hdfs://' + self.hdfs_name_node,
                '-hdfs_version=' + self.hdfs_version,
            ]

        if self.use_maprfs:
            args += [
                # 3 slashes?
                '-hdfs maprfs:///' + self.hdfs_name_node,
                '-hdfs_version=' + self.hdfs_version,
            ]

        if self.aws_credentials:
            args += ['--aws_credentials=' + self.aws_credentials]

        if self.disable_h2o_log:
            args += ['--nolog']

        # disable logging of requests, as some contain "error", which fails the test
        ## FIXED. better escape in check_sandbox_for_errors
        ## args += ['--no_requests_log']
        return args

    def __init__(self,
                 use_this_ip_addr=None, port=54321, capture_output=True,
                 use_debugger=None, classpath=None,
                 use_hdfs=False, use_maprfs=False,
                 # hdfs_version="cdh4", hdfs_name_node="192.168.1.151",
                 # hdfs_version="cdh3", hdfs_name_node="192.168.1.176",
                 hdfs_version=None, hdfs_name_node=None, hdfs_config=None,
                 aws_credentials=None,
                 use_flatfile=False, java_heap_GB=None, java_heap_MB=None, java_extra_args=None,
                 use_home_for_ice=False, node_id=None, username=None,
                 disable_h2o_log=False,
                 enable_benchmark_log=False,
                 h2o_remote_buckets_root=None,
                 delete_keys_at_teardown=False,
                 cloud_name=None,
    ):

        if use_hdfs:
            # see if we can touch a 0xdata machine
            try:
                # long timeout in ec2...bad
                a = requests.get('http://192.168.1.176:80', timeout=1)
                hdfs_0xdata_visible = True
            except:
                hdfs_0xdata_visible = False

            # different defaults, depending on where we're running
            if hdfs_name_node is None:
                if hdfs_0xdata_visible:
                    hdfs_name_node = "192.168.1.176"
                else: # ec2
                    hdfs_name_node = "10.78.14.235:9000"

            if hdfs_version is None:
                if hdfs_0xdata_visible:
                    hdfs_version = "cdh3"
                else: # ec2
                    hdfs_version = "0.20.2"

        self.aws_credentials = aws_credentials
        self.port = port
        # None is legal for self.addr.
        # means we won't give an ip to the jar when we start.
        # Or we can say use use_this_ip_addr=127.0.0.1, or the known address
        # if use_this_addr is None, use 127.0.0.1 for urls and json
        # Command line arg 'ipaddr_from_cmd_line' dominates:
        if ipaddr_from_cmd_line:
            self.addr = ipaddr_from_cmd_line
        else:
            self.addr = use_this_ip_addr

        if self.addr is not None:
            self.http_addr = self.addr
        else:
            self.http_addr = get_ip_address()

        # command line should always dominate for enabling
        if debugger: use_debugger = True
        self.use_debugger = use_debugger

        self.classpath = classpath
        self.capture_output = capture_output

        self.use_hdfs = use_hdfs
        self.use_maprfs = use_maprfs
        self.hdfs_name_node = hdfs_name_node
        self.hdfs_version = hdfs_version
        self.hdfs_config = hdfs_config

        self.use_flatfile = use_flatfile
        self.java_heap_GB = java_heap_GB
        self.java_heap_MB = java_heap_MB
        self.java_extra_args = java_extra_args

        self.use_home_for_ice = use_home_for_ice
        self.node_id = node_id

        if username:
            self.username = username
        else:
            self.username = getpass.getuser()

        # don't want multiple reports from tearDown and tearDownClass
        # have nodes[0] remember (0 always exists)
        self.sandbox_error_was_reported = False
        self.sandbox_ignore_errors = False

        self.disable_h2o_log = disable_h2o_log

        # this dumps stats from tests, and perf stats while polling to benchmark.log
        self.enable_benchmark_log = enable_benchmark_log
        self.h2o_remote_buckets_root = h2o_remote_buckets_root
        self.delete_keys_at_teardown = delete_keys_at_teardown

        if cloud_name:
            self.cloud_name = cloud_name
        else:
            self.cloud_name = 'pytest-%s-%s' % (getpass.getuser(), os.getpid())

    def __str__(self):
        return '%s - http://%s:%d/' % (type(self), self.http_addr, self.port)


#*****************************************************************
class LocalH2O(H2O):
    '''An H2O instance launched by the python framework on the local host using psutil'''

    def __init__(self, *args, **kwargs):
        super(LocalH2O, self).__init__(*args, **kwargs)
        self.rc = None
        # FIX! no option for local /home/username ..always the sandbox (LOG_DIR)
        self.ice = tmp_dir('ice.')
        self.flatfile = flatfile_name()
        self.remoteH2O = False # so we can tell if we're remote or local

        if self.node_id is not None:
            logPrefix = 'local-h2o-' + str(self.node_id)
        else:
            logPrefix = 'local-h2o'

        spawn = spawn_cmd(logPrefix, cmd=self.get_args(), capture_output=self.capture_output)
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

    def terminate_self_only(self):
        try:
            if self.is_alive(): self.ps.kill()
            if self.is_alive(): self.ps.terminate()
            return self.wait(0.5)
        except psutil.NoSuchProcess:
            return -1

    def terminate(self):
        # send a shutdown request first.
        # since local is used for a lot of buggy new code, also do the ps kill.
        # try/except inside shutdown_all now
        self.shutdown_all()
        if self.is_alive():
            print "\nShutdown didn't work for local node? : %s. Will kill though" % self
        self.terminate_self_only()

    def wait(self, timeout=0):
        if self.rc is not None:
            return self.rc
        try:
            self.rc = self.ps.wait(timeout)
            return self.rc
        except psutil.TimeoutExpired:
            return None

    def stack_dump(self):
        self.ps.send_signal(signal.SIGQUIT)

