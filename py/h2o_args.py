
import getpass, inspect, sys, argparse, unittest
from h2o_get_ip import get_ip_address

print "h2o_args"
# Global disable. used to prevent browsing when running nosetests, or when given -bd arg
# Defaults to true, if user=jenkins, h2o.unit_main isn't executed, so parse_our_args isn't executed.
# Since nosetests doesn't execute h2o.unit_main, it should have the browser disabled.

# this should be imported in full to init these before unit_main might be called
browse_disable = True
browse_json = False
verbose = False
ip_from_cmd_line = None
network_from_cmd_line = None
config_json = None
debugger = False
random_udp_drop = False
force_tcp = False
random_seed = None
beta_features = True
sleep_at_tear_down = False
abort_after_import = False
clone_cloud_json = None
disable_time_stamp = False
debug_rest = False
long_test_case = False
# jenkins gets this assign, but not the unit_main one?
# python_test_name = inspect.stack()[1][1]
python_test_name = python_test_name = inspect.stack()[1][1]
python_cmd_ip = get_ip_address(ipFromCmdLine=ip_from_cmd_line)

# no command line args if run with just nose
python_cmd_args = ""
# don't really know what it is if nosetests did some stuff. Should be just the test with no args
python_cmd_line = ""
python_username = getpass.getuser()


def parse_our_args():
    parser = argparse.ArgumentParser()
    # can add more here
    parser.add_argument('-bd', '--browse_disable',
        help="Disable any web browser stuff. Needed for batch. nosetests and jenkins disable browser through other means already, so don't need",
        action='store_true')
    parser.add_argument('-b', '--browse_json',
        help='Pops a browser to selected json equivalent urls. Selective. Also keeps test alive (and H2O alive) till you ctrl-c. Then should do clean exit',
        action='store_true')
    parser.add_argument('-v', '--verbose',
        help='increased output',
        action='store_true')
    # I guess we don't have a -port at the command line
    parser.add_argument('-ip', '--ip', type=str,
        help='IP address to use for single host H2O with psutil control')
    parser.add_argument('-network', '--network', type=str,
        help='network/mask (shorthand form) to use to resolve multiple possible IPs')
    parser.add_argument('-cj', '--config_json',
        help='Use this json format file to provide multi-host defaults. Overrides the default file pytest_config-<username>.json. These are used only if you do build_cloud_with_hosts()')
    parser.add_argument('-dbg', '--debugger',
        help='Launch java processes with java debug attach mechanisms',
        action='store_true')
    parser.add_argument('-rud', '--random_udp_drop',
        help='Drop 20 pct. of the UDP packets at the receive side',
        action='store_true')
    parser.add_argument('-s', '--random_seed', type=int,
        help='initialize SEED (64-bit integer) for random generators')
    parser.add_argument('-bf', '--beta_features',
        help='enable or switch to beta features (import2/parse2)',
        action='store_true')
    parser.add_argument('-slp', '--sleep_at_tear_down',
        help='open browser and time.sleep(3600) at tear_down_cloud() (typical test end/fail)',
        action='store_true')
    parser.add_argument('-aai', '--abort_after_import',
        help='abort the test after printing the full path to the first dataset used by import_parse/import_only',
        action='store_true')
    parser.add_argument('-ccj', '--clone_cloud_json', type=str,
        help='a h2o-nodes.json file can be passed (see build_cloud(create_json=True). This will create a cloned set of node objects, so any test that builds a cloud, can also be run on an existing cloud without changing the test')
    parser.add_argument('-dts', '--disable_time_stamp',
        help='Disable the timestamp on all stdout. Useful when trying to capture some stdout (like json prints) for use elsewhere',
        action='store_true')
    parser.add_argument('-debug_rest', '--debug_rest',
        help='Print REST API interactions to rest.log',
        action='store_true')
    parser.add_argument('-nc', '--nocolor',
        help="don't emit the chars that cause color printing",
        action='store_true')
    parser.add_argument('-long', '--long_test_case',
        help="some tests will vary behavior to more, longer cases",
        action='store_true')

    parser.add_argument('unittest_args', nargs='*')
    args = parser.parse_args()

    # disable colors if we pipe this into a file to avoid extra chars
    if args.nocolor:
        h2p.disable_colors()

    global browse_disable, browse_json, verbose, ip_from_cmd_line, config_json, debugger, random_udp_drop
    global random_seed, beta_features, sleep_at_tear_down, abort_after_import
    global clone_cloud_json, disable_time_stamp, debug_rest, long_test_case

    browse_disable = args.browse_disable or getpass.getuser() == 'jenkins'
    browse_json = args.browse_json
    verbose = args.verbose
    ip_from_cmd_line = args.ip
    network_from_cmd_line = args.network
    config_json = args.config_json
    debugger = args.debugger
    random_udp_drop = args.random_udp_drop
    random_seed = args.random_seed
    # beta_features is hardwired to True
    # beta_features = args.beta_features
    sleep_at_tear_down = args.sleep_at_tear_down
    abort_after_import = args.abort_after_import
    clone_cloud_json = args.clone_cloud_json
    disable_time_stamp = args.disable_time_stamp
    debug_rest = args.debug_rest
    long_test_case = args.long_test_case

    # Set sys.argv to the unittest args (leav sys.argv[0] as is)
    # FIX! this isn't working to grab the args we don't care about
    # Pass "--failfast" to stop on first error to unittest. and -v
    # won't get this for jenkins, since it doesn't do parse_our_args
    sys.argv[1:] = ['-v', "--failfast"] + args.unittest_args
    # sys.argv[1:] = args.unittest_args

def unit_main():
    # moved clean_sandbox out of here, because nosetests doesn't execute h2o.unit_main in our tests.
    # UPDATE: ..is that really true? I'm seeing the above print in the console output runnning
    # jenkins with nosetests
    parse_our_args()

    global python_test_name, python_cmd_args, python_cmd_line, python_cmd_ip, python_username
    # if I remember correctly there was an issue with using sys.argv[0]
    # under nosetests?. yes, see above. We just duplicate it here although sys.argv[0] might be fine here
    python_test_name = inspect.stack()[1][1]
    python_cmd_args = " ".join(sys.argv[1:])
    python_cmd_line = "python %s %s" % (python_test_name, python_cmd_args)
    python_username = getpass.getuser()
    # if test was run with nosestests, it wouldn't execute unit_main() so we won't see this
    # so this is correct, for stuff run with 'python ..."
    print "\nTest: %s    command line: %s" % (python_test_name, python_cmd_line)

    # depends on ip_from_cmd_line
    python_cmd_ip = get_ip_address(ipFromCmdLine=ip_from_cmd_line)
    unittest.main()
