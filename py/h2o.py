# refactored: 11/4/14 kbn
# if you're looking for the h2o methods on nodes, look in h2o_methods.py
# the h2o node and host classes are in h2o_objects.py
# random stuff for test that was in h2o.py is in h2o_test.py, and can be still accessed here, for test legacy
# I grepped to see which command line args and functions were used, and included here
# h2o_args.py has the command line arg parsing

# build_cloud() stuff is in h2o_bc, but build_cloud() redirects from here, and also saves h2o.nodes[] normally
# so all h2o.nodes[0].get_cloud() style stuff still works

import h2o_args
import h2o_nodes

# tests reference the first line of stuff, through h2o.*
from h2o_bc import decide_if_localhost, touch_cloud, verify_cloud_size, stabilize_cloud, \
    build_cloud as build_cloud2, \
    build_cloud_with_json as build_cloud_with_json2, \
    tear_down_cloud as tear_down_cloud2

from h2o_test import \
    make_syn_dir, tmp_file, tmp_dir, check_sandbox_for_errors, clean_sandbox, \
    clean_sandbox_stdout_stderr, \
    find_file, dump_json, sleep, spawn_cmd, spawn_cmd_and_wait, \
    spawn_wait, verboseprint, setup_random_seed, get_sandbox_name

from h2o_args import unit_main
from h2o_get_ip import get_ip_address

# print "h2o"

def setup_benchmark_log():
    # h2o.cloudPerfH2O is used in tests. Simplest to have the instance here.
    # an object to keep stuff out of h2o.py
    import h2o_perf
    global cloudPerfH2O
    global python_test_name
    cloudPerfH2O = h2o_perf.PerfH2O(python_test_name)

def copy_h2o_args_to_here():
    # if we only copy after the build cloud, the unit_main will have run (if not jenkins)
    # and no one should be looking here during import (because these won't exist yet)
    # hack to support legacy tests that look at h2o.* for these
    global beta_features, long_test_case, browse_disable, verbose, abort_after_import
    global clone_cloud_json, config_json
    global python_username, python_test_name, python_cmd_line
    # Warning: only legacy tests should use these.
    # all others should use h2o_args (and not as module globals that execute during module import)
    # to be sure the updated h2o_args is used.
    beta_features = h2o_args.beta_features
    long_test_case = h2o_args.long_test_case
    browse_disable = h2o_args.browse_disable
    verbose = h2o_args.verbose
    abort_after_import = h2o_args.abort_after_import
    clone_cloud_json = h2o_args.clone_cloud_json
    config_json = h2o_args.config_json
    python_username = h2o_args.python_username
    python_test_name = h2o_args.python_test_name
    python_cmd_line = h2o_args.python_cmd_line
    # print "python stuff in h2o:", python_username, python_test_name, python_cmd_line

# get an initial copy, in case ray looks at something before cloud building!
copy_h2o_args_to_here()
# want to keep the global state nodes here in addition to h2o_nodes.nodes[], for legacy h2o.nodes[] refs.
# empty until cloud build!
nodes = []

def build_cloud(*args, **kwargs):
    copy_h2o_args_to_here()
    global nodes
    nodes = build_cloud2(*args, **kwargs)
    # watch out with nodes. multiple copies. make sure set/cleared in sync
    # done already
    # h2o_nodes.nodes[:] = nodes

    # keep this param in kwargs, because we pass it to the H2O node build, so state
    # is created that polling and other normal things can check, to decide to dump
    # info to benchmark.log
    if kwargs.setdefault('enable_benchmark_log', False):
        setup_benchmark_log()

    return nodes

def build_cloud_with_json(*args, **kwargs):
    copy_h2o_args_to_here()
    global nodes
    nodes = build_cloud_with_json2(*args, **kwargs)

    # done already
    # h2o_nodes.nodes[:] = nodes
    return nodes

# do it this way to make sure we destory the local nodes copy too
def tear_down_cloud(*args, **kwargs):
    tear_down_cloud2(*args, **kwargs)
    h2o_nodes.nodes[:] = []
    global nodes
    nodes = []

# only usable after you've built a cloud (junit, watch out)
def cloud_name():
    return nodes[0].cloud_name

# doesn't depend on h2o_args
LOG_DIR = get_sandbox_name()

# have to wait until def build_cloud() above, because h2o_hosts will import it
# so keep the import down here
import h2o_hosts

def init(*args, **kwargs):
    global localhost
    localhost = decide_if_localhost()
    global nodes
    # we go thru the defs above, to do the other stuff like grab h2o_args
    # don't really need to assign nodes here since it's done above, and
    # build_cloud_with_hosts goes thru build_cloud. But this makes it obvious.
    if (localhost):
        nodes = build_cloud(*args, **kwargs)
    else:
        nodes = h2o_hosts.build_cloud_with_hosts(*args, **kwargs)

