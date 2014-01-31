from h2oPerf.PerformanceRunner import *
import sys
import os
import shutil
import signal
import time
import random
import getpass
import re
import subprocess

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

def use(x):
    """ Hack to remove compiler warning. """
    if False:
        print(x)

def signal_handler(signum, stackframe):
    global g_runner
    global g_handling_signal

    use(stackframe)

    if (g_handling_signal):
        # Don't do this recursively.
        return
    g_handling_signal = True

    print("")
    print("----------------------------------------------------------------------")
    print("")
    print("SIGNAL CAUGHT (" + str(signum) + ").  TEARING DOWN CLOUDS.")
    print("")
    print("----------------------------------------------------------------------")
    g_runner.terminate()


def usage():
    print("")
    print("Usage:  " + g_script_name +
          " [--wipe]"
          " [--baseport port]"
          " [--numnodes n]"
          " [--usecloud ip:port]")
    print("")
    print("    (Output dir is: " + g_output_dir + ")")
    print("")
    print("    --wipe        Wipes the output dir before starting.  Keeps old random seeds.")
    print("")
    print("    --baseport    The first port at which H2O starts searching for free ports.")
    print("")
    print("    --numnodes    The number of clouds to start.")
    print("                  Each test is randomly assigned to a cloud.")
    print("")
    print("    --usecloud    ip:port of cloud to send tests to instead of starting clouds.")
    print("                  (When this is specified, numclouds is ignored.)")
    print("")
    print("    --jvm.xmx     Configure size of launched JVM running H2O. E.g. '--jvm.xmx 3g'")
    print("")
    print("")
    print("Examples:")
    print("")
    print("    Just accept the defaults and go (note: output dir must not exist):")
    print("        "+g_script_name)
    print("")
    print("    For a powerful laptop with 8 cores (keep default numclouds):")
    print("        "+g_script_name+" --wipe")
    print("")
    print("    For a big server with 32 cores:")
    print("        "+g_script_name+" --wipe --numclouds 16")
    print("")
    print("    Run tests on a pre-existing cloud (e.g. in a debugger):")
    print("        "+g_script_name+" --wipe --usecloud ip:port")
    sys.exit(1)

def unknown_arg(s):
    print("")
    print("ERROR: Unknown argument: " + s)
    print("")
    usage()

def bad_arg(s):
    print("")
    print("ERROR: Illegal use of (otherwise valid) argument: " + s)
    print("")
    usage()

def parse_args(argv):
    global g_base_port
    global g_nodes_in_cloud
    global g_wipe_output_dir
    global g_use_cloud
    global g_use_ip
    global g_use_port
    global g_jvm_xmx

    i = 1
    while (i < len(argv)):
        s = argv[i]

        if (s == "--baseport"):
            i += 1
            if (i > len(argv)):
                usage()
            g_base_port = int(argv[i])
        elif (s == "--numnodes"):
            i += 1
            if (i > len(argv)):
                usage()
            g_nodes_in_cloud = int(argv[i])
        elif (s == "--wipe"):
            g_wipe_output_dir = True
        elif (s == "--usecloud"):
            i += 1
            if (i > len(argv)):
                usage()
            s = argv[i]
            m = re.match(r'(\S+):([1-9][0-9]*)', s)
            if (m is None):
                unknown_arg(s)
            g_use_cloud = True
            g_use_ip = m.group(1)
            port_string = m.group(2)
            g_use_port = int(port_string)
        elif (s == "--jvm.xmx"):
            i += 1
            if (i > len(argv)):
                usage()
            g_jvm_xmx = argv[i]
        elif (s == "-h" or s == "--h" or s == "-help" or s == "--help"):
            usage()
        else:
            unknown_arg(s)

        i += 1

def wipe_output_dir():
    print("")
    print("Wiping output directory...")
    try:
        if (os.path.exists(g_output_dir)):
            shutil.rmtree(g_output_dir)
    except OSError as e:
        print("")
        print("ERROR: Removing output directory failed: " + g_output_dir)
        print("       (errno {0}): {1}".format(e.errno, e.strerror))
        print("")
        sys.exit(1)

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
    g_runner.start_cloud()
    g_runner.run_tests()
    g_runner.stop_cloud()
    g_runner.report_summary()

if __name__ == "__main__":
    main(sys.argv)
