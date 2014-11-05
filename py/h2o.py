
# refactored: 11/4/14 kbn
# if you're looking for the h2o methods on nodes, look in h2o_methods.py
# the h2o node and host classes are in h2o_objects.py
# random stuff for test that was in h2o.py is in h2o_test.py, and can be still accessed here, for test legacy
# I grepped to see which command line args and functions were used, and included here
# h2o_args.py has the command line arg parsing

# build_cloud() stuff is in h2o_bc, but build_cloud() redirects from here, and also saves h2o.nodes[] normally
# so all h2o.nodes[0].get_cloud() style stuff still works

import h2o_args
import h2o_bc
import h2o_nodes
from h2o_test import get_sandbox_name

print "h2o"

# want to keep the global state nodes here
nodes = []
def build_cloud(*args, **kwargs):
    global nodes
    nodes = h2o_bc.build_cloud(*args, **kwargs)
    
    # watch out with nodes. multiple copies. make sure set/cleared in sync
    # done already
    # h2o_nodes.nodes[:] = nodes
    return nodes

def build_cloud_with_json(*args, **kwargs):
    global nodes
    nodes = h2o_bc.build_cloud_with_json(*args, **kwargs)
    
    # done already
    # h2o_nodes.nodes[:] = nodes
    return nodes

# do it this way to make sure we destory the local nodes copy too
def tear_down_cloud(*args, **kwargs):
    h2o_bc.tear_down_cloud(*args, **kwargs)
    h2o_nodes.nodes[:] = []
    global nodes
    nodes = []

# only usable after you've built a cloud (junit, watch out)
def cloud_name():
    return nodes[0].cloud_name

# these are used in existing tests
beta_features = h2o_args.beta_features
long_test_case = h2o_args.long_test_case
browse_disable = h2o_args.browse_disable
verbose = h2o_args.verbose
abort_after_import = h2o_args.abort_after_import
python_username = h2o_args.python_username
python_test_name = h2o_args.python_test_name
python_cmd_line = h2o_args.python_cmd_line

LOG_DIR = get_sandbox_name()

from h2o_bc import decide_if_localhost, touch_cloud, verify_cloud_size
from h2o_get_ip import get_ip_address

from h2o_test import \
    make_syn_dir, tmp_file, check_sandbox_for_errors, clean_sandbox, clean_sandbox_stdout_stderr, \
    find_file, dump_json, sleep, spawn_cmd, spawn_cmd_and_wait, \
    spawn_wait, verboseprint, setup_random_seed

from h2o_args import unit_main

# h2o.cloudPerfH2O is used in tests. Simplest to have the instance here.
def setup_benchmark_log():
    # an object to keep stuff out of h2o.py
    import h2o_perf
    global cloudPerfH2O
    cloudPerfH2O = h2o_perf.PerfH2O(python_test_name)
                                                         
