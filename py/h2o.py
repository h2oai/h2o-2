import time, os, stat, json, signal, tempfile, shutil, datetime, inspect, threading, getpass
import requests, psutil, argparse, sys, unittest, glob
import h2o_browse as h2b, h2o_perf, h2o_util, h2o_cmd, h2o_os_util
import h2o_sandbox
import h2o_print as h2p
import re, webbrowser, random
# used in shutil.rmtree permission hack for windows
import errno
# use to unencode the urls sent to h2o?
import urlparse
import logging
# for log_download
import requests, zipfile, StringIO

# For checking ports in use, using netstat thru a subprocess.
from subprocess import Popen, PIPE
import stat

class OutWrapper:
    def __init__(self, out):
        self._out = out
    def write(self, x):
        self._out.write(x.replace('\n', '\n[{0}] '.format(str(datetime.datetime.now()))))
    def flush(self):
        self._out.flush()

def check_params_update_kwargs(params_dict, kw, function, print_params):
    # only update params_dict..don't add
    # throw away anything else as it should come from the model (propagating what RF used)
    for k in kw:
        if k in params_dict:
            params_dict[k] = kw[k]
        else:
            raise Exception("illegal parameter '%s' in %s" % (k, function))

    if print_params:
        print "\n%s parameters:" % function, params_dict
        sys.stdout.flush()

def verboseprint(*args, **kwargs):
    if verbose:
        for x in args: # so you don't have to create a single string
            print x,
        for x in kwargs: # so you don't have to create a single string
            print x,
        print
        # so we can see problems when hung?
        sys.stdout.flush()

def sleep(secs):
    if getpass.getuser()=='jenkins':
        period = max(secs,120)
    else:
        period = secs
    # if jenkins, don't let it sleep more than 2 minutes
    # due to left over h2o.sleep(3600)
    time.sleep(period)

# The cloud is uniquely named per user (only)
# Fine to uniquely identify the flatfile by name only also?
# Both are the user that runs the test. The config might have a different username on the
# remote machine (0xdiag, say, or hduser)
def flatfile_name():
    return('pytest_flatfile-%s' %getpass.getuser())

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
    t = threading.Thread(target=__drain, args=(src,dst))
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

def get_sandbox_name():
    if os.environ.has_key("H2O_SANDBOX_NAME"): 
        return os.environ["H2O_SANDBOX_NAME"]
    else: 
        return "sandbox"

def unit_main():
    global python_test_name, python_cmd_args, python_cmd_line, python_cmd_ip, python_username
    # if I remember correctly there was an issue with using sys.argv[0]
    # under nosetests?. yes, see above. We just duplicate it here although sys.argv[0] might be fine here
    python_test_name = inspect.stack()[1][1]
    python_cmd_ip = get_ip_address()
    python_cmd_args = " ".join(sys.argv[1:])
    python_cmd_line = "python %s %s" % (python_test_name, python_cmd_args)
    python_username = getpass.getuser()
    # if test was run with nosestests, it wouldn't execute unit_main() so we won't see this
    # so this is correct, for stuff run with 'python ..."
    print "\nTest: %s    command line: %s" % (python_test_name, python_cmd_line)
    print "Python runs on: %s" % python_cmd_ip

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
ipaddr_from_cmd_line = None
config_json = None
debugger = False
random_udp_drop = False
random_seed = None
beta_features = False
sleep_at_tear_down = False
abort_after_import = False
clone_cloud_json = None
disable_time_stamp = False
debug_rest = False
# jenkins gets this assign, but not the unit_main one?
python_test_name = inspect.stack()[1][1]
python_cmd_ip = get_ip_address()
# no command line args if run with just nose
python_cmd_args = ""
# don't really know what it is if nosetests did some stuff. Should be just the test with no args
python_cmd_line = ""
python_username = getpass.getuser()

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
    parser.add_argument('-s', '--random_seed', type=int, help='initialize SEED (64-bit integer) for random generators')
    parser.add_argument('-bf', '--beta_features', help='enable or switch to beta features (import2/parse2)', action='store_true')
    parser.add_argument('-slp', '--sleep_at_tear_down', help='open browser and time.sleep(3600) at tear_down_cloud() (typical test end/fail)', action='store_true')
    parser.add_argument('-aai', '--abort_after_import', help='abort the test after printing the full path to the first dataset used by import_parse/import_only', action='store_true')
    parser.add_argument('-ccj', '--clone_cloud_json', type=str, help='a h2o-nodes.json file can be passed (see build_cloud(create_json=True). This will create a cloned set of node objects, so any test that builds a cloud, can also be run on an existing cloud without changing the test')
    parser.add_argument('-dts', '--disable_time_stamp', help='Disable the timestamp on all stdout. Useful when trying to capture some stdout (like json prints) for use elsewhere', action='store_true')
    parser.add_argument('-debug_rest', '--debug_rest', help='Print REST API interactions to rest.log', action='store_true')

    parser.add_argument('unittest_args', nargs='*')

    args = parser.parse_args()
    global browse_disable, browse_json, verbose, ipaddr_from_cmd_line, config_json, debugger, random_udp_drop
    global random_seed, beta_features, sleep_at_tear_down, abort_after_import, clone_cloud_json, disable_time_stamp, debug_rest

    browse_disable = args.browse_disable or getpass.getuser()=='jenkins'
    browse_json = args.browse_json
    verbose = args.verbose
    ipaddr_from_cmd_line = args.ip
    config_json = args.config_json
    debugger = args.debugger
    random_udp_drop = args.random_udp_drop
    random_seed = args.random_seed
    beta_features = args.beta_features
    sleep_at_tear_down = args.sleep_at_tear_down
    abort_after_import = args.abort_after_import
    clone_cloud_json = args.clone_cloud_json
    disable_time_stamp = args.disable_time_stamp
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

LOG_DIR = get_sandbox_name()

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
    fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=LOG_DIR)
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

def kill_process_tree(pid, including_parent=True):
    parent = psutil.Process(pid)
    for child in parent.get_children(recursive=True):
        child.kill()
    if including_parent:
        parent.kill()

def kill_child_processes():
    me = os.getpid()
    kill_process_tree(me, including_parent=False)

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
            hostPortList.append(ip + ":" + str(base_port + ports_per_node*i))
    else:
        for h in hosts:
            for i in range(node_count):
                # removed leading "/"
                hostPortList.append(h.addr + ":" + str(base_port + ports_per_node*i))

    # note we want to shuffle the full list of host+port
    if rand_shuffle:
        random.shuffle(hostPortList)
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

# node_count is number of H2O instances per host if hosts is specified.
def decide_if_localhost():
    # First, look for local hosts file
    hostsFile = default_hosts_file()
    if config_json:
        print "* Using config JSON you passed as -cj argument:", config_json
        return False
    if os.path.exists(hostsFile):
        print "* Using config JSON file discovered in this directory: {0}.".format(hostsFile)
        return False
    if 'hosts' in os.getcwd():
        print "Since you're in a *hosts* directory, we're using a config json"
        print "* Expecting default username's config json here. Better exist!"
        return False
    print "No config json used. Launching local cloud..."
    return True

def setup_random_seed(seed=None):
    if random_seed is not None:
        SEED = random_seed
    elif seed is not None:
        SEED = seed
    else:
        SEED = random.randint(0, sys.maxint)
    random.seed(SEED)
    print "\nUsing random seed:", SEED
    return SEED

# assume h2o_nodes_json file in the current directory
def build_cloud_with_json(h2o_nodes_json='h2o-nodes.json'):
    log("#*********************************************************************")
    log("Starting new test: " + python_test_name + " at build_cloud_with_json()")
    log("#*********************************************************************")

    print "This only makes sense if h2o is running as defined by", h2o_nodes_json
    print "For now, assuming it's a cloud on this machine, and here's info on h2o processes running here"
    print "No output means no h2o here! Some other info about stuff on the system is printed first though."
    import h2o_os_util
    if not os.path.exists(h2o_nodes_json):
        raise Exception("build_cloud_with_json: Can't find "+h2o_nodes_json+" file")

    # h2o_os_util.show_h2o_processes()

    with open(h2o_nodes_json, 'rb') as f:
        cloneJson = json.load(f)

    # These are supposed to be in the file.
    # Just check the first one. if not there, the file must be wrong
    if not 'cloud_start' in cloneJson:
        raise Exception("Can't find 'cloud_start' in %s, wrong file? h2o-nodes.json?" % h2o_nodes_json)
    else:
        cs = cloneJson['cloud_start']
        print "Info on the how the cloud we're cloning was apparently started (info from %s)" % h2o_nodes_json
        # required/legal values in 'cloud_start'. A robust check is good for easy debug when we add stuff
        # for instance, if you didn't get the right/latest h2o-nodes.json! (we could check how old the cloud start is?)
        valList = ['time', 'cwd', 'python_test_name', 'python_cmd_line', 'config_json', 'username', 'ip']
        for v in valList:
            if v not in cs:
                raise Exception("Can't find %s in %s, wrong file or version change?" % (v, h2o_nodes_json))
            print "cloud_start['%s']: %s" % (v, cs[v])

        # write out something that shows how the cloud could be rebuilt, since it's a decoupled cloud build.
###         build_cloud_rerun_sh = LOG_DIR + "/" + 'build_cloud_rerun.sh'
###         with open(build_cloud_rerun_sh, 'w') as f:
###             f.write("echo << ! > ./temp_for_build_cloud_rerun.sh\n")
###             f.write("echo 'Rebuilding a cloud built with %s at %s by %s on %s in %s'\n" % \
###                 (cs['python_test_name'], cs['time'], cs['username'], cs['ip'], cs['cwd']))
###             f.write("cd %s\n" % cs['cwd'])
###             if cs['config_json']:
###                 f.write("%s -cj %s\n" % (cs['python_cmd_line'], cs['config_json']))
###             else:
###                 f.write("%s\n" % cs['python_cmd_line'])
###             f.write("!\n")
###             f.write("ssh %s@%s < ./temp_for_build_cloud_rerun.sh\n" % (cs['username'], cs['ip']))
###         # make it executable
###         t = os.stat(build_cloud_rerun_sh)
###         os.chmod(build_cloud_rerun_sh, t.st_mode | stat.S_IEXEC)

        # this is the internal node state for python..h2o.nodes rebuild
        nodeStateList = cloneJson['h2o_nodes']

    nodeList = []
    for nodeState in nodeStateList:
        print "Cloning state for node", nodeState['node_id'], 'from', h2o_nodes_json

        newNode = ExternalH2O(nodeState)
        nodeList.append(newNode)

    print ""
    h2p.red_print("Ingested from json:", nodeList[0].java_heap_GB, "GB java heap(s) with", len(nodeList), "total nodes")
    print ""
    nodes[:] = nodeList
    # put the test start message in the h2o log, to create a marker
    nodes[0].h2o_log_msg()
    return nodeList

def setup_benchmark_log():
    # an object to keep stuff out of h2o.py
    global cloudPerfH2O
    cloudPerfH2O = h2o_perf.PerfH2O(python_test_name)

# node_count is per host if hosts is specified.
def build_cloud(node_count=1, base_port=54321, hosts=None,
    timeoutSecs=30, retryDelaySecs=1, cleanup=True, rand_shuffle=True,
    conservative=False, create_json=False, clone_cloud=None, **kwargs):
    # redirect to build_cloud_with_json if a command line arg
    # wants to force a test to ignore it's build_cloud/build_cloud_with_hosts
    # (both come thru here)
    # clone_cloud is just another way to get the effect (maybe ec2 config file thru
    # build_cloud_with_hosts?
    if not disable_time_stamp:
        sys.stdout = OutWrapper(sys.stdout)
    if clone_cloud_json or clone_cloud:
        nodeList = build_cloud_with_json(
            h2o_nodes_json=clone_cloud_json if clone_cloud_json else clone_cloud)
        return nodeList

    # moved to here from unit_main. so will run with nosetests too!
    clean_sandbox()
    log("#*********************************************************************")
    log("Starting new test: " + python_test_name + " at build_cloud()")
    log("#*********************************************************************")

    # start up h2o to report the java version (once). output to python stdout
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
        portList = [base_port + ports_per_node*i for i in range(node_count)]
        if hosts is None:
            # if use_flatfile, we should create it,
            # because tests will just call build_cloud with use_flatfile=True
            # best to just create it all the time..may or may not be used
            write_flatfile(node_count=node_count, base_port=base_port)
            hostCount = 1
            if rand_shuffle: 
                random.shuffle(portList)
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
                    hostPortList.append( (h,port) )
            if rand_shuffle: random.shuffle(hostPortList)
            for (h,p) in hostPortList:
                verboseprint('ssh starting node', totalNodes, 'via', h)
                newNode = h.remote_h2o(port=p, node_id=totalNodes, **kwargs)
                nodeList.append(newNode)
                totalNodes += 1

        verboseprint("Attempting Cloud stabilize of", totalNodes, "nodes on", hostCount, "hosts")
        start = time.time()
        # UPDATE: best to stabilize on the last node!
        stabilize_cloud(nodeList[0], len(nodeList),
            timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, noExtraErrorCheck=True)
        verboseprint(len(nodeList), "Last added node stabilized in ", time.time()-start, " secs")
        verboseprint("Built cloud: %d nodes on %d hosts, in %d s" % (len(nodeList),
            hostCount, (time.time() - start)))

        # FIX! using "consensus" in node[-1] should mean this is unnecessary?
        # maybe there's a bug. For now do this. long term: don't want?
        # UPDATE: do it for all cases now 2/14/13
        if conservative: # still needed?
            for n in nodeList:
                stabilize_cloud(n, len(nodeList), timeoutSecs=timeoutSecs, noExtraErrorCheck=True)
        else:
            pass
            # verify_cloud_size(nodeList)

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
        shutil.copy(config_json,  LOG_DIR + "/" + os.path.basename(config_json))

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

###     # write out something that shows how the test could be rerun (could be a cloud build, a mix, or test only)
###     print "Writing the test_rerun.sh in", LOG_DIR
###     test_rerun_sh = LOG_DIR + "/" + 'test_rerun.sh'
###     with open(test_rerun_sh, 'w') as f:
###         f.write("echo << ! > ./temp_for_test_rerun.sh\n")
###         f.write("echo 'rerunning %s that originally ran at %s by %s on %s in %s'\n" % \
###                 (cs_python_test_name, cs_time, cs_username, cs_ip, cs_cwd))
###         f.write("cd %s\n" % cs_cwd)
###         if cs_config_json:
###             f.write("%s -cj %s\n" % (cs_python_cmd_line, cs_config_json))
###         else:
###             f.write("%s\n" % cs_python_cmd_line)
###         f.write("!\n")
###         f.write("ssh %s@%s < temp_for_test_rerun.sh\n" % (cs_username, cs_ip))
### 
###     # make it executable
###     t = os.stat(test_rerun_sh)
###     os.chmod(test_rerun_sh, t.st_mode | stat.S_IEXEC)

# dump the h2o.nodes state to a json file # include enough extra info to have someone rebuild the cloud if a test fails
    # that was using that cloud.
    if create_json:
        q = {
                'cloud_start':
                    {
                    'time': cs_time,
                    'cwd': cs_cwd,
                    'python_test_name': cs_python_test_name,
                    'python_cmd_line': cs_python_cmd_line,
                    'config_json': cs_config_json,
                    'username': cs_username,
                    'ip': cs_ip,
                    },
                'h2o_nodes': h2o_util.json_repr(nodes),
            }

        with open('h2o-nodes.json', 'w+') as f:
            f.write(json.dumps(q, indent=4))

    return nodeList

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
    if sleep_at_tear_down:
        print "Opening browser to cloud, and sleeping for 3600 secs, before cloud teardown (for debug)"
        import h2o_browse
        h2b.browseTheCloud()
        sleep(3600)

    if not nodeList: nodeList = nodes
    try:
        for n in nodeList:
            n.terminate()
            verboseprint("tear_down_cloud n:", n)
    finally:
        check_sandbox_for_errors(sandboxIgnoreErrors=sandboxIgnoreErrors, python_test_name=python_test_name)
        nodeList[:] = []

# don't need any more?
# Used before to make sure cloud didn't go away between unittest defs
def touch_cloud(nodeList=None):
    if not nodeList: nodeList = nodes
    for n in nodeList:
        n.is_alive()

# timeoutSecs is per individual node get_cloud()
def verify_cloud_size(nodeList=None, verbose=False, timeoutSecs=10):
    if not nodeList: nodeList = nodes

    expectedSize = len(nodeList)
    # cloud size and consensus have to reflect a single grab of information from a node.
    cloudStatus = [n.get_cloud(timeoutSecs=timeoutSecs) for n in nodeList]
    cloudSizes = [c['cloud_size'] for c in cloudStatus]
    cloudConsensus = [c['consensus'] for c in cloudStatus]

    if expectedSize==0 or len(cloudSizes)==0 or len(cloudConsensus)==0:
        print "\nexpectedSize:", expectedSize
        print "cloudSizes:", cloudSizes
        print "cloudConsensus:", cloudConsensus
        raise Exception("Nothing in cloud. Can't verify size")

    for s in cloudSizes:
        consensusStr = (",".join(map(str,cloudConsensus)))
        sizeStr =   (",".join(map(str,cloudSizes)))
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
        consensus  = c['consensus']
        locked     = c['locked']
        cloud_size = c['cloud_size']
        cloud_name = c['cloud_name']
        node_name  = c['node_name']

        if 'nodes' not in c:
            emsg = "\nH2O didn't include a list of nodes in get_cloud response after initial cloud build"
            raise Exception(emsg)

        # only print it when you get consensus
        if cloud_size!=node_count:
            verboseprint("\nNodes in cloud while building:")
            for ci in c['nodes']:
                verboseprint(ci['name'])

        if (cloud_size > node_count):
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

        a = (cloud_size==node_count) and consensus
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
        if loc.startswith('/'): delim = ''
        else: delim = '/'
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
            paramsStr =  '?' + '&'.join(['%s=%s' % (k,v) for (k,v) in params.items()])
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
            if cmd=='post':
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
            if not noExtraErrorCheck: # use this to ignore the initial connection errors during build cloud when h2o is coming up
                h2p.red_print("ERROR: got exception on %s to h2o. \nGoing to check sandbox, then rethrow.." % (url + paramsStr))
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
            # Paranoid exception catch.  Ignore logging exceptions in the case that the above error checking isn't sufficient.
            pass

        # fatal if no response
        if not beta_features and not r:
            raise Exception("Maybe bad url? no r in __do_json_request in %s:" % inspect.stack()[1][3])

        # this is used to open a browser on results, or to redo the operation in the browser
        # we don't' have that may urls flying around, so let's keep them all
        json_url_history.append(r.url)
        if not beta_features and not r.json():
            raise Exception("Maybe bad url? no r.json in __do_json_request in %s:" % inspect.stack()[1][3])

        rjson = None
        if returnFast:
            return
        try:
            rjson = r.json()
        except:
            print dump_json(r.text)
            if not isinstance(r,(list,dict)): 
                raise Exception("h2o json responses should always be lists or dicts, see previous for text")

            raise Exception("Could not decode any json from the request. Do you have beta features turned on? beta_features: ", beta_features)

        for e in ['error', 'Error', 'errors', 'Errors']:
            # error can be null (python None). This happens in exec2
            if e in rjson and rjson[e]:
                verboseprint(dump_json(rjson))
                emsg = 'rjson %s in %s: %s' % (e, inspect.stack()[1][3], rjson[e])
                if ignoreH2oError:
                    # well, we print it..so not totally ignore. test can look at rjson returned
                    print emsg
                else:
                    raise Exception(emsg)

        for w in ['warning', 'Warning', 'warnings', 'Warnings']:
            # warning can be null (python None).
            if w in rjson and rjson[w]:
                verboseprint(dump_json(rjson))
                print 'rjson %s in %s: %s' % (w, inspect.stack()[1][3], rjson[w])

        return rjson


    def test_redirect(self):
        return self.__do_json_request('TestRedirect.json')

    def test_poll(self, args):
        return self.__do_json_request('TestPoll.json', params=args)

    #FIX! just here temporarily to get the response at the end of an algo, from job/destination_key
    def completion_redirect(self, jsonRequest, params):
        return self.__do_json_request(jsonRequest=jsonRequest, params=params)

    def get_cloud(self, noExtraErrorCheck=False, timeoutSecs=10):
        # hardwire it to allow a 60 second timeout
        a = self.__do_json_request('Cloud.json', noExtraErrorCheck=noExtraErrorCheck, timeout=timeoutSecs)

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

    def h2o_log_msg(self, message=None):
        if 1==0:
            return
        if not message:
            message = "\n"
            message += "\n#***********************"
            message += "\npython_test_name: " + python_test_name
            message += "\n#***********************"
        params = {'message': message}
        self.__do_json_request('2/LogAndEcho', params=params)

    def get_timeline(self):
        return self.__do_json_request('Timeline.json')

    # Shutdown url is like a reset button. Doesn't send a response before it kills stuff
    # safer if random things are wedged, rather than requiring response
    # so request library might retry and get exception. allow that.
    def shutdown_all(self):
        try:
            self.__do_json_request('Shutdown.json', noExtraErrorCheck=True)
        except:
            pass
        time.sleep(1) # a little delay needed?
        return(True)

    def put_value(self, value, key=None, repl=None):
        return self.__do_json_request(
            'PutValue.json',
            params={"value": value, "key": key, "replication_factor": repl},
            extraComment = str(value) + "," + str(key) + "," + str(repl))

    def put_file(self, f, key=None, timeoutSecs=60):
        if key is None:
            key = os.path.basename(f)
            ### print "putfile specifying this key:", key

        resp = self.__do_json_request(
                '2/PostFile.json' if beta_features else 'PostFile.json',
                cmd='post',
                timeout=timeoutSecs,
                params={"key": key},
                files={"file": open(f, 'rb')},
                extraComment = str(f))

        verboseprint("\nput_file response: ", dump_json(resp))
        return key

    def get_key(self, key, timeoutSecs=30):
        params = {'key': key}
        paramsStr =  '?' + '&'.join(['%s=%s' % (k,v) for (k,v) in params.items()])
        url = self.__url('Get.html')
        log('Start ' + url + paramsStr)

        # how big does the data return get? I guess only use on relatively small file/key
        return requests.get(url, params=params, timeout=timeoutSecs)

    # noise is a 2-tuple ("StoreView", none) for url plus args for doing during poll to create noise
    # so we can create noise with different urls!, and different parms to that url
    # no noise if None
    def poll_url(self, response,
        timeoutSecs=10, retryDelaySecs=0.5, initialDelaySecs=0, pollTimeoutSecs=180,
        noise=None, benchmarkLogging=None, noPoll=False, reuseFirstPollUrl=False):
        ### print "poll_url: pollTimeoutSecs", pollTimeoutSecs
        verboseprint('poll_url input: response:', dump_json(response))

        # for the rev 2 stuff..the job_key, destination_key and redirect_url are just in the response
        # look for 'response'..if not there, assume the rev 2

        def get_redirect_url(response, beta_features):
            url = None
            params = None
            # StoreView has old style, while beta_features
            if 'response_info' in response: # trigger v2 for GBM always?
                response_info = response['response_info']

                if 'redirect_url' not in response_info:
                    raise Exception("Response during polling must have 'redirect_url'\n%s" % dump_json(response))

                if response_info['status'] != 'done':
                    redirect_url = response_info['redirect_url']
                    # HACK: these are missing the "2/" prefix for now
                    # 'KMeans2Progress' in str(redirect_url) or
                    # 'GLMModelView' in str(redirect_url) or
###                     if  'NeuralNetProgress' in str(redirect_url) or \
###                         'PCAProgressPage' in str(redirect_url):
###                         if "2/" not in str(redirect_url):
###                             print "Hacking in the 2/ prefix..need to fix h2o?"
###                             redirect_url = "2/" + redirect_url
###                     if  'DRFProgressPage' in str(redirect_url):
###                         if "2/" not in str(redirect_url):
###                             # already has a leading /?
###                             print "Hacking in the 2/ prefix..need to fix h2o?"
###                             redirect_url = "2" + redirect_url

                    if redirect_url:
                        url = self.__url(redirect_url)
                        params = None
                    else:
                        if response_info['status'] != 'done':
                            raise Exception("'redirect_url' during polling is null but status!='done': \n%s" % dump_json(response))
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
            # default to "" if doesn't exist
        else:
            status = response['response']['status']
        progress = response.get('progress', "")
        doFirstPoll = status != 'done'

        (url, params) = get_redirect_url(response, beta_features)
        # no need to recreate the string for messaging, in the loop..
        if params:
            paramsStr = '&'.join(['%s=%s' % (k,v) for (k,v) in params.items()])
        else:
            paramsStr = ''

        # FIX! don't do JStack noise for tests that ask for it. JStack seems to have problems
        noise_enable = noise is not None and noise != ("JStack", None)
        if noise_enable:
            print "Using noise during poll_url:", noise
            # noise_json should be like "Storeview"
            (noise_json, noiseParams) = noise
            noiseUrl = self.__url(noise_json + ".json")
            if noiseParams is None:
                noiseParamsStr = ""
            else:
                noiseParamsStr =  '&'.join(['%s=%s' % (k,v) for (k,v) in noiseParams.items()])

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
        while status == 'poll' or doFirstPoll or (beta_features and status == 'redirect' and 'Inspect' not in url):
            count += 1
            if ((time.time()-start) > timeoutSecs):
                # show what we're polling with
                emsg = "Exceeded timeoutSecs: %d secs while polling." % timeoutSecs +\
                       "status: %s, url: %s?%s" % (status, urlUsed, paramsUsedStr)
                raise Exception(emsg)

            if benchmarkLogging:
                cloudPerfH2O.get_log_save(benchmarkLogging)

            print status, progress, url
            time.sleep(retryDelaySecs)
            # every other one?
            create_noise = noise_enable and ((count%2)==0)
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

            response = self.__do_json_request(fullUrl=urlUsed, timeout=pollTimeoutSecs, params=paramsUsed)
            verboseprint(msgUsed, urlUsed, paramsUsedStr, "Response:", dump_json(response))
            # hey, check the sandbox if we've been waiting a long time...rather than wait for timeout
            if ((count%6)==0):
                check_sandbox_for_errors(python_test_name=python_test_name)

            if (create_noise):
                # this guarantees the loop is done, so we don't need to worry about
                # a 'return r' being interpreted from a noise response
                status = 'poll'
                progress = ''
            else:
                doFirstPoll = False
                if beta_features or 'response_info' in response: # trigger v2 for GBM always?
                    status = response['response_info']['status']
                else:
                    status = response['response']['status']

                progress = response.get('progress', "")

                # get the redirect url
                if beta_features and not reuseFirstPollUrl: # reuse url for all v1 stuff
                    (url, params) = get_redirect_url(response, beta_features)

                if noPoll:
                    return response

        # won't print if we didn't poll
        if msgUsed:
            verboseprint(msgUsed, urlUsed, paramsUsedStr, "Response:", dump_json(response))
        return response

    def kmeans_apply(self, data_key, model_key, destination_key,
        timeoutSecs=300, retryDelaySecs=0.2, initialDelaySecs=None, pollTimeoutSecs=180,
        **kwargs):
        # defaults
        params_dict = {
            'destination_key': destination_key,
            'model_key': model_key,
            'data_key': data_key,
            }
        browseAlso = kwargs.get('browseAlso', False)
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'kmeans_apply', print_params=True)

        print "\nKMeansApply params list:", params_dict
        a = self.__do_json_request('KMeansApply.json', timeout=timeoutSecs, params=params_dict)

        # Check that the response has the right Progress url it's going to steer us to.
        if a['response']['redirect_request']!='Progress':
            print dump_json(a)
            raise Exception('H2O kmeans redirect is not Progress. KMeansApply json response precedes.')
        a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
            initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
        verboseprint("\nKMeansApply result:", dump_json(a))

        if (browseAlso | browse_json):
            print "Redoing the KMeansApply through the browser, no results saved though"
            h2b.browseJsonHistoryAsUrlLastMatch('KMeansApply')
            time.sleep(5)
        return a

    # model_key
    # key
    def kmeans_score(self, key, model_key,
        timeoutSecs=300, retryDelaySecs=0.2, initialDelaySecs=None, pollTimeoutSecs=180,
        **kwargs):
        # defaults
        params_dict = {
            'key': key,
            'model_key': model_key,
            }
        browseAlso = kwargs.get('browseAlso', False)
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'kmeans_score', print_params=True)
        print "\nKMeansScore params list:", params_dict
        a = self.__do_json_request('KMeansScore.json', timeout=timeoutSecs, params=params_dict)

        # kmeans_score doesn't need polling?
        verboseprint("\nKMeansScore result:", dump_json(a))

        if (browseAlso | browse_json):
            print "Redoing the KMeansScore through the browser, no results saved though"
            h2b.browseJsonHistoryAsUrlLastMatch('KMeansScore')
            time.sleep(5)
        return a

    # this is only for 2 (fvec)
    def kmeans_model_view(self, model, timeoutSecs=30, **kwargs):
        # defaults
        params_dict = {
            'model': model,
            }
        browseAlso = kwargs.get('browseAlso', False)
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'kmeans_model_view', print_params=True)
        print "\nKMeans2ModelView params list:", params_dict
        a = self.__do_json_request('2/KMeans2ModelView.json', timeout=timeoutSecs, params=params_dict)

        # kmeans_score doesn't need polling?
        verboseprint("\nKMeans2Model View result:", dump_json(a))

        if (browseAlso | browse_json):
            print "Redoing the KMeans2ModelView through the browser, no results saved though"
            h2b.browseJsonHistoryAsUrlLastMatch('KMeans2ModelView')
            time.sleep(5)
        return a

    # additional params include: cols=.
    # don't need to include in params_dict it doesn't need a default
    # FIX! cols should be renamed in test for fvec
    def kmeans(self, key, key2=None, 
        timeoutSecs=300, retryDelaySecs=0.2, initialDelaySecs=None, pollTimeoutSecs=180,
        noise=None, benchmarkLogging=None, noPoll=False, **kwargs):
        # defaults
        # KMeans has more params than shown here
        # KMeans2 has these params?
        # max_iter=100&max_iter2=1&iterations=0
        if beta_features:
            params_dict = {
                'initialization': 'Furthest',
                'k': 1,
                'source': key,
                'destination_key': key2,
                'seed': None,
                'ignored_cols': None,
                'max_iter': None,
                'normalize': None,
                }
        else:
            params_dict = {
                'initialization': 'Furthest',
                'k': 1,
                'source_key': key,
                'destination_key': key2,
                'seed': None,
                'cols': None,
                'max_iter': None,
                'normalize': None,
                }

        if key2 is not None: params_dict['destination_key'] = key2
        browseAlso = kwargs.get('browseAlso', False)
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'kmeans', print_params=True)
        algo = '2/KMeans2' if beta_features else 'KMeans'

        print "\n%s params list:" % algo, params_dict
        a = self.__do_json_request(algo + '.json', 
            timeout=timeoutSecs, params=params_dict)

        if noPoll:
            return a


        a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
            initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
            noise=noise, benchmarkLogging=benchmarkLogging)
        verboseprint("\n%s result:" % algo, dump_json(a))

        if (browseAlso | browse_json):
            print "Redoing the %s through the browser, no results saved though" % algo
            h2b.browseJsonHistoryAsUrlLastMatch(algo)
            time.sleep(5)
        return a

    # params:
    # header=1,
    # header_from_file
    # separator=1 (hex encode?
    # exclude=
    # noise is a 2-tuple: ("StoreView",params_dict)

    def parse(self, key, key2=None,
        timeoutSecs=300, retryDelaySecs=0.2, initialDelaySecs=None, pollTimeoutSecs=180,
        noise=None, benchmarkLogging=None, noPoll=False, **kwargs):
        browseAlso = kwargs.pop('browseAlso',False)
        # this doesn't work. webforums indicate max_retries might be 0 already? (as of 3 months ago)
        # requests.defaults({max_retries : 4})
        # https://github.com/kennethreitz/requests/issues/719
        # it was closed saying Requests doesn't do retries. (documentation implies otherwise)
        algo = "2/Parse2" if beta_features else "Parse"
        verboseprint("\n %s key: %s to key2: %s (if None, means default)" % (algo, key, key2))
        # other h2o parse parameters, not in the defauls
        # header
        # exclude
        params_dict = {
            'blocking': None, # debug only
            'source_key': key, # can be a regex
            'destination_key': key2,
            'parser_type': None,
            'separator': None,
            'header': None,
            'single_quotes': None,
            'header_from_file': None,
            'exclude': None,
            'preview': None,
            }
        check_params_update_kwargs(params_dict, kwargs, 'parse', print_params=True)

        # h2o requires header=1 if header_from_file is used. Force it here to avoid bad test issues
        if kwargs.get('header_from_file'): # default None
            kwargs['header'] = 1

        if benchmarkLogging:
            cloudPerfH2O.get_log_save(initOnly=True)

        a = self.__do_json_request(algo + ".json", timeout=timeoutSecs, params=params_dict)

        # Check that the response has the right Progress url it's going to steer us to.
        verboseprint(algo + " result:", dump_json(a))

        # FIX! not using h2o redirect info for Parse2 yet
        if not beta_features and a['response']['redirect_request']!='Progress':
            raise Exception('H2O %s redirect is not Progress. %s json response precedes.' % (algo, algo) )

        if noPoll:
            return a

        # noise is a 2-tuple ("StoreView, none) for url plus args for doing during poll to create noise
        # no noise if None
        verboseprint(algo + ' noise:', noise)
        a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
            initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
            noise=noise, benchmarkLogging=benchmarkLogging)

        verboseprint("\n" + algo + " result:", dump_json(a))
        return a

    def netstat(self):
        return self.__do_json_request('Network.json')

    def jstack(self, timeoutSecs=30):
        return self.__do_json_request("JStack.json", timeout=timeoutSecs)

    def iostatus(self):
        return self.__do_json_request("IOStatus.json")

    # &offset=
    # &view=
    def inspect(self, key, offset=None, view=None, max_column_display=1000, ignoreH2oError=False, 
        timeoutSecs=30, useVA=False):
        if beta_features and not useVA:
            params = {
                "src_key": key,
                "offset": offset,
                # view doesn't exist for 2. let it be passed here from old tests but not used
                }
        else:
            params = {
                "key": key,
                "offset": offset,
                "view": view,
                "max_column_display": max_column_display
                }

        a = self.__do_json_request('2/Inspect2.json' if (beta_features and not useVA) else 'Inspect.json',
            params=params,
            ignoreH2oError=ignoreH2oError,
            timeout=timeoutSecs
            )
        return a

    # can take a useful 'filter'
    # FIX! current hack to h2o to make sure we get "all" rather than just
    # default 20 the browser gets. set to max # by default (1024)
    # There is a offset= param that's useful also, and filter=
    def store_view(self, timeoutSecs=60, print_params=False, **kwargs):
        params_dict = {
            'view': 20,
            }
        params_dict.update(kwargs)
        if print_params:
            print "\nStoreView params list:", params_dict

        a = self.__do_json_request('StoreView.json',
            params=params_dict,
            timeout=timeoutSecs)
        return a

    # There is also a RemoveAck in the browser, that asks for confirmation from
    # the user. This is after that confirmation.
    # UPDATE: ignore errors on remove..key might already be gone due to h2o removing it now
    # after parse
    def remove_key(self, key, timeoutSecs=120):
        a = self.__do_json_request('Remove.json', 
            params={"key": key}, ignoreH2oError=True, timeout=timeoutSecs)
        return a

    # this removes all keys!
    def remove_all_keys(self, timeoutSecs=120):
        a = self.__do_json_request('2/RemoveAll.json', timeout=timeoutSecs)
        return a

    # only model keys can be exported?
    def export_hdfs(self, source_key, path):
        a = self.__do_json_request('ExportHdfs.json',
            params={"source_key": source_key, "path": path})
        verboseprint("\nexport_hdfs result:", dump_json(a))
        return a

    def export_s3(self, source_key, bucket, obj):
        a = self.__do_json_request('ExportS3.json',
            params={"source_key": source_key, "bucket": bucket, "object": obj})
        verboseprint("\nexport_s3 result:", dump_json(a))
        return a

    # the param name for ImportFiles is 'file', but it can take a directory or a file.
    # 192.168.0.37:54323/ImportFiles.html?file=%2Fhome%2F0xdiag%2Fdatasets
    def import_files(self, path, timeoutSecs=180):
        a = self.__do_json_request('2/ImportFiles2.json' if beta_features else 'ImportFiles.json',
            timeout=timeoutSecs,
            params={"path": path}
        )
        verboseprint("\nimport_files result:", dump_json(a))
        return a

    def import_s3(self, bucket, timeoutSecs=180):
        a = self.__do_json_request('2/ImportFiles2.json' if beta_features else 'ImportS3.json', 
            timeout=timeoutSecs, params={"bucket": bucket})
        verboseprint("\nimport_s3 result:", dump_json(a))
        return a

    def import_hdfs(self, path, timeoutSecs=180):
        a = self.__do_json_request('2/ImportFiles2.json' if beta_features else 'ImportHdfs.json', 
            timeout=timeoutSecs, params={"path": path})
        verboseprint("\nimport_hdfs result:", dump_json(a))
        return a

    # 'destination_key', 'escape_nan' 'expression'
    def exec_query(self, timeoutSecs=20, ignoreH2oError=False, **kwargs):
        # only v2 now
        params_dict = {
            'str': None,
            }

        browseAlso = kwargs.pop('browseAlso',False)
        check_params_update_kwargs(params_dict, kwargs, 'exec_query', print_params=True)
        verboseprint("\nexec_query:", params_dict)
        a = self.__do_json_request('2/Exec2.json',
            timeout=timeoutSecs, ignoreH2oError=ignoreH2oError, params=params_dict)
        verboseprint("\nexec_query result:", dump_json(a))
        return a

    def jobs_admin(self, timeoutSecs=20, **kwargs):
        params_dict = {
            # 'expression': None,
            }
        browseAlso = kwargs.pop('browseAlso',False)
        params_dict.update(kwargs)
        verboseprint("\nexec_query:", params_dict)
        a = self.__do_json_request('Jobs.json', timeout=timeoutSecs, params=params_dict)
        verboseprint("\njobs_admin result:", dump_json(a))
        return a

    def jobs_cancel(self, timeoutSecs=20, **kwargs):
        params_dict = {
            'key': None,
            }
        browseAlso = kwargs.pop('browseAlso',False)
        check_params_update_kwargs(params_dict, kwargs, 'jobs_cancel', print_params=True)
        a = self.__do_json_request('Cancel.json', timeout=timeoutSecs, params=params_dict)
        verboseprint("\njobs_cancel result:", dump_json(a))
        print "Cancelled job:", params_dict['key']
    
        return a

    # note ntree in kwargs can overwrite trees! (trees is legacy param)
    def random_forest(self, data_key, trees, 
        timeoutSecs=300, retryDelaySecs=1.0, initialDelaySecs=None, pollTimeoutSecs=180,
        noise=None, benchmarkLogging=None, print_params=True, noPoll=False, rfView=True, **kwargs):

        algo = '2/DRF' if beta_features else 'RF'
        algoView = '2/DRFView' if beta_features else 'RFView'

        if beta_features:
            params_dict = {
                'destination_key': None,
                'source': data_key,
                # 'model': None,
                'response': None,
                'cols': None,
                'ignored_cols_by_name': None,
                'classification': None,
                'validation': None,
                'importance': None, # enable variable importance
                'ntrees': trees,
                'max_depth': None,
                'min_rows': None, # how many rows in leaves for stopping condition
                'nbins': None,
                'mtries': None,
                'sample_rate': None,
                'seed': None,
                }
            if 'model_key' in kwargs:
                kwargs['destination_key'] = kwargs['model_key'] # hmm..should we switch test to new param?


        else:
            params_dict = {
                'data_key': data_key,
                'ntree':  trees,
                'model_key': None,
                # new default. h2o defaults to 0, better for tracking oobe problems
                'out_of_bag_error_estimate': 1,
                'use_non_local_data': None,
                'iterative_cm': None,
                'response_variable': None,
                'class_weights': None,
                'stat_type': None,
                'depth': None,
                'bin_limit': None,
                'ignore': None,
                'sample': None,
                'seed': None,
                'features': None,
                'exclusive_split_limit': None,
                'sampling_strategy': None,
                'strata_samples': None,
        }

        browseAlso = kwargs.pop('browseAlso',False)
        check_params_update_kwargs(params_dict, kwargs, 'random_forest', print_params)

        if beta_features and params_dict['response'] is None:
            # on v2, there is no default response. So if it's none, we should use the last column, for compatibility
            inspect = h2o_cmd.runInspect(key=data_key)
            # response only takes names. can't use col index..have to look it up
            params_dict['response'] = str(inspect['cols'][-1]['name'])

        if print_params:
            print "\n%s parameters:" % algo, params_dict
            sys.stdout.flush()

        # always follow thru to rfview?
        rf = self.__do_json_request(algo + '.json', timeout=timeoutSecs, params=params_dict)
        print "\n%s result:" % algo, dump_json(rf)
        
        # noPoll and rfView=False are similar?
        if (noPoll or not rfView) or (beta_features and rfView==False):
            # just return for now
            print "no rfView:", rfView, "noPoll", noPoll
            return rf

        if beta_features:
            # since we don't know the model key from the rf response, we just let rf redirect us to completion
            # if we want to do noPoll, we have to name the model, so we know what to ask for when we do the completion view
            # HACK: wait more for first poll?
            time.sleep(5)
            rfView = self.poll_url(rf, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
                initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
                noise=noise, benchmarkLogging=benchmarkLogging)
            return rfView

        else:
            if rf['response']['redirect_request'] != algoView:
                print dump_json(rf)
                raise Exception('H2O %s redirect is not %s json response precedes.' % (algo, algoView))


            # FIX! check all of these somehow?
            # if we model_key was given to rf via **kwargs, remove it, since we're passing 
            # model_key from rf. can't pass it in two places. (ok if it doesn't exist in kwargs)
            data_key  = rf['data_key']
            model_key = rf['model_key']
            rfCloud = rf['response']['h2o']
            # this is important. it's the only accurate value for how many trees RF was asked for.
            ntree    = rf['ntree']
            response_variable = rf['response_variable']

            # Since I may not have passed a model_key or ntree to h2o, I have to learn what h2o used
            # and be sure to pass that to RFView. just update params_dict. If I provided them
            # I'm trusting h2o to have given them back to me correctly. Maybe fix that at some point.
            params_dict.update({'ntree': ntree, 'model_key': model_key})

            # data_key/model_key/ntree are all in **params_dict
            rfViewResult = self.random_forest_view(timeoutSecs=timeoutSecs, 
                retryDelaySecs=retryDelaySecs, initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
                noise=noise, benchmarkLogging=benchmarkLogging, print_params=print_params, noPoll=noPoll, 
                useRFScore=False, **params_dict) 

            verboseprint("random_forest_view:", rfViewResult)
            return rfViewResult

    def random_forest_view(self, data_key=None, model_key=None, timeoutSecs=300, 
            retryDelaySecs=0.2, initialDelaySecs=None, pollTimeoutSecs=180,
            noise=None, benchmarkLogging=None, print_params=False, noPoll=False, 
            useRFScore=False, **kwargs): 

        # not supported yet
        if beta_features:
            print "random_forest_view not supported in H2O fvec yet. hacking done response"
            r = {'response': {'status': 'done'}, 'trees': {'number_built': 0}}
            # return r

        # algo = '2/DRFView2' if beta_features else 'RFView'
        # for drf2, you can't pass a new dataset here, compared to what you trained with.
        # should complain or something if tried with a data_key
        if beta_features and data_key:
            print "Can't pass a new data_key to random_forest_view for v2's DRFModelView. Not using"

        algo = '2/DRFModelView' if beta_features else 'RFView'
        # No such thing as 2/DRFScore2
        algoScore = '2/DRFScore2' if beta_features else 'RFScore'
        # is response_variable needed here? it shouldn't be
        # do_json_request will ignore any that remain = None

        if beta_features:
            params_dict = {
                '_modelKey': model_key,
                }
        else:
            params_dict = {
                'data_key': data_key,
                'model_key': model_key,
                'out_of_bag_error_estimate': 1,
                'iterative_cm': 0,
                # is ntree not expected here?
                'ntree': None,
                'class_weights': None,
                'response_variable': None,
                }
        browseAlso = kwargs.pop('browseAlso',False)

        # only update params_dict..don't add
        # throw away anything else as it should come from the model (propagating what RF used)
        for k in kwargs:
            if k in params_dict:
                params_dict[k] = kwargs[k]

        if beta_features:
            ntree = 0 # not used?

        else:
            if params_dict['ntree'] is None:
                raise Exception('%s got no %s param: %s . Why? h2o needs?' % (algo, 'ntree', params_dict['ntree']))
        # well assume we got the gold standard from the initial rf request
            ntree = params_dict['ntree']

        if print_params:
            print "\n%s parameters:" % algo, params_dict
            sys.stdout.flush()

        if useRFScore:
            whichUsed = algoScore
        else:
            whichUsed = algo

        a = self.__do_json_request(whichUsed + ".json", timeout=timeoutSecs, params=params_dict)
        verboseprint("\n%s result:" % whichUsed, dump_json(a))

        if noPoll:
            return a

        # add a fake redirect_request and redirect_request_args
        # to the RF response, to make it look like everyone else
        if beta_features:
            rfView = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
                initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
                noise=noise, benchmarkLogging=benchmarkLogging)
        else:
            fake_a = a
            fake_a['response'] = a['response']
            fake_a['response']['redirect_request'] = whichUsed + ".json"
            fake_a['response']['redirect_request_args'] = params_dict

            # no redirect_response in rfView? so need to pass params here
            # FIX! do we have to do a 2nd if it's done in the first?
            rfView = self.poll_url(fake_a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
                initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
                noise=noise, benchmarkLogging=benchmarkLogging, reuseFirstPollUrl=True)

            # above we get this from what we're told from rf and passed to rfView
            ## ntree = rfView['ntree']
            verboseprint("%s done:" % whichUsed, dump_json(rfView))
            status = rfView['response']['status']
            if status != 'done': raise Exception('Unexpected status: ' + status)

        if beta_features:
            drf_model = rfView['drf_model']
            numberBuilt = drf_model['N']
        else:
            pass
            # rfView seems to have two ways of returning the number built
            # this doesn't work when we call it directly?
            # numberBuilt = rfView['trees']['number_built']
            # if numberBuilt!=ntree:
            #     raise Exception("%s done but number_built!=ntree: %s %s" % (whichUsed, numberBuilt, ntree))

        # want to double check all this because it's new
        # and we had problems with races/doneness before
        errorInResponse = False
            # numberBuilt<0 or ntree<0 or numberBuilt>ntree or \
            # ntree!=rfView['ntree']

        if errorInResponse:
            raise Exception("\nBad values in %s.json\n" % whichUsed +
                "progress: %s, progressTotal: %s, ntree: %s, numberBuilt: %s, status: %s" % \
                (progress, progressTotal, ntree, numberBuilt, status))

        if (browseAlso | browse_json):
            h2b.browseJsonHistoryAsUrlLastMatch(whichUsed)
        return rfView

    def random_forest_score(self, data_key, model_key,
        timeoutSecs=60, retryDelaySecs=0.5, initialDelaySecs=None, pollTimeoutSecs=180,
        **kwargs):
        rfView = random_forest_view(useRFScore=True, *args, **kwargs)
        return rfView

    def set_column_names(self, timeoutSecs=300, print_params=False, **kwargs):
        params_dict = {
            'source': None,
            'target': None,
        }
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'set_column_names', print_params)
        a = self.__do_json_request('SetColumnNames.json', timeout=timeoutSecs, params=params_dict)
        verboseprint("\nset_column_names result:", dump_json(a))
        return a

    def gbm_view(self, model_key, timeoutSecs=300, print_params=False, **kwargs):
        params_dict = {
            '_modelKey': model_key,
        }
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'gbm_view', print_params)
        a = self.__do_json_request('2/GBMModelView.json', timeout=timeoutSecs, params=params_dict)
        verboseprint("\ngbm_view result:", dump_json(a))
        return a

    def gbm_grid_view(self, timeoutSecs=300, print_params=False, **kwargs):
        params_dict = {
            'job_key': None,
            'destination_key': None,
        }
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'gbm_search_progress', print_params)
        a = self.__do_json_request('2/GridSearchProgress.json', timeout=timeoutSecs, params=params_dict)
        print "\ngbm_search_progress result:", dump_json(a)
        return a

    def pca_view(self, modelKey, timeoutSecs=300, print_params=False, **kwargs):
        #this function is only for pca on fvec! may replace in future.
        params_dict = {
           '_modelKey' : modelKey,
        }
        check_params_update_kwargs(params_dict, kwargs, 'pca_view', print_params)
        a = self.__do_json_request('2/PCAModelView.json', timeout=timeoutSecs, params=params_dict)
        verboseprint("\npca_view_result:", dump_json(a))
        return a

    def glm_view(self, modelKey=None, timeoutSecs=300, print_params=False, **kwargs):
        #this function is only for glm2, may remove it in future.
        params_dict = {
            '_modelKey' : modelKey,
        }
        check_params_update_kwargs(params_dict, kwargs, 'glm_view', print_params)
        a = self.__do_json_request('2/GLMModelView.json', timeout=timeoutSecs, params=params_dict)
        verboseprint("\nglm_view result:", dump_json(a))
        return a

    def generate_predictions(self, data_key, model_key, destination_key=None, timeoutSecs=300, print_params=True, **kwargs):
        algo = '2/Predict' if beta_features else 'GeneratePredictionsPage'
        algoView = '2/Inspect2' if beta_features else 'Inspect'

        if beta_features:
            params_dict = {
                'data': data_key,
                'model': model_key,
                # 'prediction_key': destination_key,
                'prediction': destination_key,
                }
        else:
            params_dict = {
                'data_key': data_key,
                'model_key': model_key,
                'destination_key': destination_key,
                }

        browseAlso = kwargs.pop('browseAlso',False)
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'generate_predictions', print_params)

        if print_params:
            print "\n%s parameters:" % algo, params_dict
            sys.stdout.flush()

        a = self.__do_json_request(
            algo + '.json',
            timeout=timeoutSecs, 
            params=params_dict)
        verboseprint("\n%s result:" % algo, dump_json(a))

        if (browseAlso | browse_json):
            h2b.browseJsonHistoryAsUrlLastMatch(algo)

        # it will redirect to an inspect, so let's get that inspect stuff
        # FIX! not supported in json yet if beta_features
        # FIX! who checks the redirect is correct?
        if beta_features:
            print "%s response:" % algo, dump_json(a)
        else:
            resultKey = a['response']['redirect_request_args']['key']
            a = self.__do_json_request(algoView, timeout=timeoutSecs, params={"key": resultKey})
            verboseprint("\nInspect of " + resultKey, dump_json(a))

        return a

    def predict_confusion_matrix(self, timeoutSecs=300, print_params=True, **kwargs):
        params_dict = {
            'actual': None,
            'vactual': 'predict',
            'predict': None,
            'vpredict': 'predict',
        }
        # everyone should move to using this, and a full list in params_dict
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'predict_confusion_matrix', print_params)
        a = self.__do_json_request('2/ConfusionMatrix.json', timeout=timeoutSecs, params=params_dict)
        verboseprint("\nprediction_confusion_matrix result:", dump_json(a))
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

        a = self.__do_json_request('RFTreeView.json', timeout=timeoutSecs, params=params_dict,
            ignoreH2oError=ignoreH2oError)

        verboseprint("\nrandom_forest_treeview result:", dump_json(a))
        # Always do it to eyeball?
        if (browseAlso | browse_json | True):
            h2b.browseJsonHistoryAsUrlLastMatch("RFTreeView")
            time.sleep(3) # to be able to see it
        return a

    def gbm(self, data_key, timeoutSecs=600, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30, 
        noPoll=False, print_params=True, **kwargs):
        params_dict = {
            'destination_key'      : None,
            'validation'           : None,
            'response'             : None,
            'source'               : data_key,
            'learn_rate'           : None,
            'ntrees'               : None,
            'max_depth'            : None,
            'min_rows'             : None,
            'ignored_cols_by_name' : None, # either this or cols..not both
            'cols'                 : None,
            'nbins'                : None,
            'classification'       : None,
            'score_each_iteration' : None,
            'grid_parallelism'     : None,
        }

        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'gbm', print_params)
        if 'validation' not in kwargs:
            kwargs['validation'] = data_key

        start = time.time()
        a = self.__do_json_request('2/GBM.json',timeout=timeoutSecs,params=params_dict)
        if noPoll:
            a['python_elapsed'] = time.time() - start
            a['python_%timeout'] = a['python_elapsed']*100 / timeoutSecs
            return a

        verboseprint("\nGBM first result:", dump_json(a))
        a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
                          initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
        verboseprint("\nGBM result:", dump_json(a))
        a['python_elapsed'] = time.time() - start
        a['python_%timeout'] = a['python_elapsed']*100 / timeoutSecs
        return a

    def pca(self, data_key, timeoutSecs=600, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30, 
        noPoll=False, print_params=True, benchmarkLogging=None, returnFast=False, **kwargs):
        params_dict = {
            'destination_key' : None,
            'source'          : data_key,
            'ignored_cols'    : None,
            'tolerance'       : None,
            'max_pc'          : None,
            'standardize'     : None,
        }
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'pca', print_params)
        start = time.time()
        a = self.__do_json_request('2/PCA.json', timeout=timeoutSecs, params=params_dict, returnFast=returnFast)

        if noPoll:
            #a['python_elapsed'] = time.time() - start
            #a['python_%timeout'] = a['python_elapsed']*100 / timeoutSecs
            return a

        a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs, benchmarkLogging=benchmarkLogging,
                          initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
        verboseprint("\nPCA result:", dump_json(a))
        a['python_elapsed'] = time.time() - start
        a['python_%timeout'] = a['python_elapsed']*100 / timeoutSecs
        return a

    def pca_score(self, timeoutSecs=600, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30, 
        noPoll=False, print_params=True, **kwargs):
        params_dict = {
            'model'           : None,
            'destination_key' : None,
            'source'          : None,
            'num_pc'          : None,
        }
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'pca_score', print_params)
        start = time.time()
        a = self.__do_json_request('2/PCAScore.json',timeout=timeoutSecs, params=params_dict)

        if noPoll:
            a['python_elapsed'] = time.time() - start
            a['python_%timeout'] = a['python_elapsed']*100 / timeoutSecs
            return a

        if 'response' not in a:
            raise Exception("Can't tell where to go..No 'response' key in this polled json response: %s" % a)

        a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
                          initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
        verboseprint("\nPCAScore result:", dump_json(a))
        a['python_elapsed'] = time.time() - start
        a['python_%timeout'] = a['python_elapsed']*100 / timeoutSecs
        return a

    def neural_net_score(self, key, model, timeoutSecs=600, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30, 
        noPoll=False, print_params=True, **kwargs):
        params_dict = {
            'source': key,
            'destination_key': None,
            'model': model,
            # this is ignore??
            'cols': None,
            'ignored_cols': None,
            'classification': None,
            'response': None,
            'max_rows': 0,
        }
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'neural_net_score', print_params)

        start = time.time()
        a = self.__do_json_request('2/NeuralNetScore.json',timeout=timeoutSecs, params=params_dict)

        if noPoll:
            a['python_elapsed'] = time.time() - start
            a['python_%timeout'] = a['python_elapsed']*100 / timeoutSecs
            return a

        # no polling
        # a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
        #                   initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
        verboseprint("\nneural net score result:", dump_json(a))
        a['python_elapsed'] = time.time() - start
        a['python_%timeout'] = a['python_elapsed']*100 / timeoutSecs
        return a

    def neural_net(self, data_key, timeoutSecs=600, retryDelaySecs=1, initialDelaySecs=5, pollTimeoutSecs=30, 
        noPoll=False, print_params=True, **kwargs):
        params_dict = {
            'destination_key': None,
            'source': data_key,
            # this is ignore??
            'cols': None,
            'ignored_cols': None,
            'validation': None,
            'classification': None,
            'response': None,
            'activation': None,
            'hidden': None,
            'rate': None,
            'l2': None,
            'epochs': None,
        }
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'neural_net', print_params)
        if 'validation' not in kwargs:
            kwargs['validation'] = data_key

        start = time.time()
        a = self.__do_json_request('2/NeuralNet.json',timeout=timeoutSecs, params=params_dict)

        if noPoll:
            a['python_elapsed'] = time.time() - start
            a['python_%timeout'] = a['python_elapsed']*100 / timeoutSecs
            return a

        a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
                          initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs)
        verboseprint("\nneural_net result:", dump_json(a))
        a['python_elapsed'] = time.time() - start
        a['python_%timeout'] = a['python_elapsed']*100 / timeoutSecs
        return a

    def summary_page(self, key, timeoutSecs=60, noPrint=True, useVA=False, **kwargs):
        if beta_features and not useVA:
            params_dict = {
                'source': key,
                'cols': None,
                'max_ncols': 1000,
                }
        else:
            params_dict = {
                'key': key,
                'x': None,
                'max_column_display': 1000,
                }
        browseAlso = kwargs.pop('browseAlso',False)
        check_params_update_kwargs(params_dict, kwargs, 'summary_page', print_params=True)
        a = self.__do_json_request('2/SummaryPage2.json' if (beta_features and not useVA) else 'SummaryPage.json', 
            timeout=timeoutSecs, params=params_dict)
        verboseprint("\nsummary_page result:", dump_json(a))
        
        # FIX!..not there yet for 2
        # if not beta_features:
        h2o_cmd.infoFromSummary(a, noPrint=noPrint)
        return a

    def log_view(self, timeoutSecs=10, **kwargs):
        browseAlso = kwargs.pop('browseAlso',False)
        a = self.__do_json_request('LogView.json', timeout=timeoutSecs)
        verboseprint("\nlog_view result:", dump_json(a))
        if (browseAlso | browse_json):
            h2b.browseJsonHistoryAsUrlLastMatch("LogView")
            time.sleep(3) # to be able to see it
        return a

    def csv_download(self, src_key, csvPathname, timeoutSecs=60, **kwargs):
        # log it
        params = {'src_key': src_key}
        paramsStr =  '?' + '&'.join(['%s=%s' % (k,v) for (k,v) in params.items()])
        url = self.__url('2/DownloadDataset.json')
        log('Start ' + url + paramsStr, comment=csvPathname)

        # do it (absorb in 1024 byte chunks)
        r = requests.get(url, params=params, timeout=timeoutSecs)
        print "csv_download r.headers:", r.headers
        if r.status_code == 200:
            f = open(csvPathname, 'wb')
            for chunk in r.iter_content(1024):
                f.write(chunk)
        print csvPathname, "size:", h2o_util.file_size_formatted(csvPathname)

    # shouldn't need params
    def log_download(self, logDir=None, timeoutSecs=5, **kwargs):
        if logDir == None:
            logDir = LOG_DIR # normally sandbox

        url = self.__url('LogDownload.json')
        log('Start ' + url);
        print "\nDownloading h2o log(s) using:", url
        r = requests.get(url, timeout=timeoutSecs, **kwargs)
        if not r or not r.ok:
            raise Exception("Maybe bad url? no r in log_download %s in %s:" % inspect.stack()[1][3])

        z = zipfile.ZipFile(StringIO.StringIO(r.content))
        print "z.namelist:", z.namelist()
        print "z.printdir:", z.printdir()

        nameList = z.namelist()
        # the first is the h2ologs dir name.
        h2oLogDir = logDir + "/" + nameList.pop(0)

        # it's a zip of zipped files
        z = zipfile.ZipFile(StringIO.StringIO(r.content))
        z.extractall(logDir)
        # unzipped file should be in LOG_DIR now
        for zname in nameList:
            resultList = h2o_util.flat_unzip(logDir + "/" + zname, logDir)
        return resultList

    # kwargs used to pass many params
    def GLM_shared(self, key,
        timeoutSecs=300, retryDelaySecs=0.5, initialDelaySecs=None, pollTimeoutSecs=180,
        parentName=None, **kwargs):

        browseAlso = kwargs.pop('browseAlso',False)
        if beta_features:
            params_dict = {
                'source': key,
                'destination_key': None,
                'response': None,
                # what about cols? doesn't exist?
                # what about ignored_cols_by_name
                'ignored_cols': None,
                'max_iter': None,
                'standardize': None,
                'family': None,
                # 'link': None, # apparently we don't get this for GLM2
                'alpha': None,
                'lambda': None,
                'beta_epsilon': None, # GLMGrid doesn't use this name
                'tweedie_variance_power': None,
                'n_folds': None,
                'case_mode': None,
                'case_val': None, 
                # 'weight': None,
                # 'thresholds': None,
                # only GLMGrid has this..we should complain about it on GLM?
                'parallelism': None,
                'beta_eps': None,
                'classification': None,
            } 
        else:
            params_dict = {
                'key': key,
                'destination_key': None,
                'x': None,
                'y': None,
                'max_iter': None,
                'standardize': None,
                'family': None,
                'link': None,
                'alpha': None,
                'lambda': None,
                'beta_epsilon': None, # GLMGrid doesn't use this name
                'tweedie_power': None,
                'n_folds': None,
                'case_mode': None,
                'case': None, 
                'weight': None,
                'lsm_solver': None,
                'expert_settings': None,
                'thresholds': None,
                # only GLMGrid has these..we should complain about it on GLM?
                'parallelism': None,
                'beta_eps': None,
            }

        check_params_update_kwargs(params_dict, kwargs, parentName, print_params=True)
        a = self.__do_json_request(parentName + '.json', timeout=timeoutSecs, params=params_dict)
        verboseprint(parentName, dump_json(a))
        return a

    def GLM(self, key,
        timeoutSecs=300, retryDelaySecs=0.5, initialDelaySecs=None, pollTimeoutSecs=180,
        noise=None, benchmarkLogging=None, noPoll=False, destination_key='GLM_model_python_0_default_0',**kwargs):
        parentName = "2/GLM2" if beta_features else "GLM"
        a = self.GLM_shared(key, timeoutSecs, retryDelaySecs, initialDelaySecs, parentName=parentName ,destination_key=destination_key, **kwargs)
        # Check that the response has the right Progress url it's going to steer us to.
        if noPoll:
            return a
        
        if not beta_features:
            if a['response']['redirect_request']!='GLMProgressPage':
                print dump_json(a)
                raise Exception('H2O GLM redirect is not GLMProgressPage. GLM json response precedes.')
        else:
            print dump_json(a)

        if noPoll:
            return a

        a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
            initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
            noise=noise, benchmarkLogging=benchmarkLogging)
        verboseprint("GLM done:", dump_json(a))

        browseAlso = kwargs.get('browseAlso', False)
        if (browseAlso | browse_json):
            print "Viewing the GLM result through the browser"
            h2b.browseJsonHistoryAsUrlLastMatch('GLMProgressPage')
            time.sleep(5)
        return a

    def GLMGrid(self, key,
        timeoutSecs=300, retryDelaySecs=1.0, initialDelaySecs=None, pollTimeoutSecs=180,
        noise=None, benchmarkLogging=None, noPoll=False, **kwargs):

        a = self.GLM_shared(key, timeoutSecs, retryDelaySecs, initialDelaySecs, parentName="GLMGrid", **kwargs)

        # Check that the response has the right Progress url it's going to steer us to.
        if not beta_features:
            if a['response']['redirect_request']!='GLMGridProgress':
                print dump_json(a)
                raise Exception('H2O GLMGrid redirect is not GLMGridProgress. GLMGrid json response precedes.')
        else:
            print dump_json(a)

        if noPoll:
            return a

        a = self.poll_url(a, timeoutSecs=timeoutSecs, retryDelaySecs=retryDelaySecs,
            initialDelaySecs=initialDelaySecs, pollTimeoutSecs=pollTimeoutSecs,
            noise=noise, benchmarkLogging=benchmarkLogging)
        verboseprint("GLMGrid done:", dump_json(a))

        browseAlso = kwargs.get('browseAlso', False)
        if (browseAlso | browse_json):
            print "Viewing the GLM grid result through the browser"
            h2b.browseJsonHistoryAsUrlLastMatch('GLMGridProgress')
            time.sleep(5)
        return a

    def GLMGrid_view(self, timeoutSecs=300, print_params=False, **kwargs):
        params_dict = {
            'job': None,
            'destination_key': None,
        }
        # only lets these params thru
        check_params_update_kwargs(params_dict, kwargs, 'GLMGridProgress', print_params)
        a = self.__do_json_request('GLMGridProgress.json', timeout=timeoutSecs, params=params_dict)
        print "\nGLMGridProgress result:", dump_json(a)
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
        print "\nGLMScore params list:", params_dict

        a = self.__do_json_request('GLMScore.json', timeout=timeoutSecs, params=params_dict)
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
            args += [ '-Xms%dG' % self.java_heap_GB ]
            args += [ '-Xmx%dG' % self.java_heap_GB ]

        if self.java_heap_MB is not None:
            if not (1 <= self.java_heap_MB <= 256000):
                raise Exception('java_heap_MB <1 or >256000  (MB): %s' % (self.java_heap_MB))
            args += [ '-Xms%dm' % self.java_heap_MB ]
            args += [ '-Xmx%dm' % self.java_heap_MB ]

        if self.java_extra_args is not None:
            args += [ '%s' % self.java_extra_args ]


        args += ["-ea"]

        if self.use_maprfs:
            args += ["-Djava.library.path=/opt/mapr/lib"]

        if self.classpath:
            entries = [ find_file('build/classes'), find_file('lib/javassist.jar') ]
            entries += glob.glob(find_file('lib')+'/*/*.jar')
            entries += glob.glob(find_file('lib')+'/*/*/*.jar')
            args += ['-classpath', os.pathsep.join(entries), 'water.Boot']
        else:
            args += ["-jar", self.get_h2o_jar()]

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
        if self.hdfs_config:
            args += [
                '-hdfs_config ' + self.hdfs_config
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
            args += [ '--aws_credentials='+self.aws_credentials ]

        # passed thru build_cloud in test, or global from commandline arg
        if self.random_udp_drop or random_udp_drop:
            args += ['--random_udp_drop']

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
        random_udp_drop=False,
        redirect_import_folder_to_s3_path=None,
        redirect_import_folder_to_s3n_path=None,
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
                    hdfs_version =  "0.20.2"

        self.redirect_import_folder_to_s3_path = redirect_import_folder_to_s3_path
        self.redirect_import_folder_to_s3n_path = redirect_import_folder_to_s3n_path

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

        self.random_udp_drop = random_udp_drop
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

        h2o_os_util.check_port_group(self.port)
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

#*****************************************************************
class RemoteHost(object):
    def upload_file(self, f, progress=None):
        # FIX! we won't find it here if it's hdfs://192.168.1.151/ file
        f = find_file(f)
        if f not in self.uploaded:
            start = time.time()
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
                    ### print "\n{0:.3f} seconds".format(time.time() - start)
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

        # kbn. trying 9/23/13. Never specify -ip on java command line for multi-node
        # but self.addr is used elsewhere. so look at self.remoteH2O to disable in get_args()
        self.addr = addr
        self.http_addr = addr
        self.username = username # this works, but it's host state
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


#*****************************************************************
class RemoteH2O(H2O):
    '''An H2O instance launched by the python framework on a specified host using openssh'''
    def __init__(self, host, *args, **kwargs):
        super(RemoteH2O, self).__init__(*args, **kwargs)

        self.remoteH2O = True # so we can tell if we're remote or local
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
        if 1==0: # enable if you want windows remote machines
            cmdList = ["cd /tmp"] # separate by ;<space> when we join
            cmdList += ["ls -ltr " + self.jar]
            cmdList += [re.sub("/tmp/", "", cmd)]
            self.channel.exec_command("; ".join(cmdList))
        else:
            self.channel.exec_command(cmd)

        if self.capture_output:
            if self.node_id is not None:
                logPrefix = 'remote-h2o-' + str(self.node_id)
            else:
                logPrefix = 'remote-h2o'

            logPrefix += '-' + host.addr

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
            self.get_cloud(noExtraErrorCheck=True)
            return True
        except:
            return False

    def terminate_self_only(self):
        self.channel.close()
        time.sleep(1) # a little delay needed?
        # kbn: it should be dead now? want to make sure we don't have zombies
        # we should get a connection error. doing a is_alive subset.
        try:
            gc_output = self.get_cloud(noExtraErrorCheck=True)
            raise Exception("get_cloud() should fail after we terminate a node. It isn't. %s %s" % (self, gc_output))
        except:
            return True

    def terminate(self):
        self.shutdown_all()
        self.terminate_self_only()

#*****************************************************************
class ExternalH2O(H2O):
    '''A cloned H2O instance assumed to be created by others, that we can interact with via json requests (urls)
       Gets initialized with state from json created by another build_cloud, so all methods should work 'as-if"
       the cloud was built by the test (normally).
       The normal build_cloud() parameters aren't passed here, the final node state is! (and used for init)
       The list should be complete, as long as created by build_cloud(create_json=True) or
       build_cloud_with_hosts(create_json=True)
       Obviously, no psutil or paramiko work done here.
    '''
    def __init__(self, nodeState):
        for k,v in nodeState.iteritems():
            verboseprint("init:", k, v)
            # hack because it looks like the json is currently created with "None" for values of None
            # rather than worrying about that, just translate "None" to None here. "None" shouldn't exist
            # for any other reason.
            if v == "None":
                v = None
            elif v == "false":
                v = False
            elif v == "true":
                v = True
            # leave "null" as-is (string) for now?
                    
            setattr(self, k, v) # achieves self.k = v
        ## print "Cloned", len(nodeState), "things for a h2o node"

    def is_alive(self):
        verboseprint("Doing is_alive check for ExternalH2O")
        try:
            self.get_cloud()
            return True
        except:
            return False

    # no terminate_self_only method
    def terminate_self_only(self):
        raise Exception("terminate_self_only() not supported for ExternalH2O")

    def terminate(self):
        self.shutdown_all()
