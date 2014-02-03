from h2oPerf.PerformanceRunner import *
from h2oPerf.Table import *
from h2oPerf.RunnerUtils import *

import sys
import os
import shutil
import signal
import time
import random
import getpass
import re
import subprocess
import argparse

# Global variables that can be set by the user.
g_script_name = ""
g_base_port = 40000
g_nodes_in_cloud = 1 
g_wipe_output_dir = False
g_use_cloud = False
g_use_ip = None
g_use_port = None
g_jvm_xmx = "10g"

# Global variables that are set internally.
g_output_dir = None
g_runner = None
g_handling_signal = False

#parse args


def main(argv):
    """
    Main program.

    @return: none
    """
    global g_script_name
    global g_nodes_in_cloud
    global g_output_dir
    global g_test_to_run
    global g_test_list_file
    global g_test_group
    global g_runner
    global g_wipe_output_dir

    g_script_name = os.path.basename(argv[0])
    test_root_dir = os.path.dirname(os.path.realpath(__file__))
    test_root_dir = os.path.join(test_root_dir, "tests")

    # Calculate global variables.
    g_output_dir = os.path.join(os.path.dirname(test_root_dir), str("results"))
    test_root_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "tests")

    # Calculate and set other variables.
    h2o_jar = os.path.abspath(
        os.path.join(os.path.join(os.path.join(os.path.join(
            test_root_dir, ".."), ".."), "target"), "h2o.jar"))

    # Override any defaults with the user's choices.
    parse_args(argv)

    # Wipe output directory if requested.
    if g_wipe_output_dir:
        wipe_output_dir()

    g_runner = PerfRunner(test_root_dir, g_output_dir, g_nodes_in_cloud, g_jvm_xmx, h2o_jar, g_use_cloud,
                                 g_use_ip, g_use_port, g_base_port)

    # Build test list.
    g_runner.build_test_list()

    # Handle killing the runner.
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Sanity check existence of H2O jar file before starting the cloud.
    if (not os.path.exists(h2o_jar)):
        print("")
        print("ERROR: H2O jar not found: " + h2o_jar)
        print("")
        sys.exit(1)

    # Run.
    #g_runner.start_cloud()
    g_runner.run_tests()
    #g_runner.stop_cloud()
    g_runner.report_summary()

if __name__ == "__main__":
    main(sys.argv)
