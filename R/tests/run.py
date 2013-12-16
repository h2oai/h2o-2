#!/usr/bin/python

import sys
import os
import shutil
import signal
import time
import random
import getpass
import re
import subprocess


class H2OCloudNode:
    """
    A class representing one node in an H2O cloud.
    Note that the base_port is only a request for H2O.
    H2O may choose to ignore our request and pick any port it likes.
    So we have to scrape the real port number from stdout as part of cloud startup.

    port: The actual port chosen at run time.
    pid: The process id of the node.
    output_file_name: Where stdout and stderr go.  They are merged.
    child: subprocess.Popen object.
    terminated: Only from a signal.  Not normal shutdown.
    """

    def __init__(self, cloud_num, nodes_per_cloud, node_num, cloud_name, h2o_jar, base_port, xmx, output_dir):
        """
        Create a node in a cloud.

        @param cloud_num: Dense 0-based cloud index number.
        @param nodes_per_cloud: How many H2O java instances are in a cloud.  Clouds are symmetric.
        @param node_num: This node's dense 0-based node index number.
        @param cloud_name: The H2O -name command-line argument.
        @param h2o_jar: Path to H2O jar file.
        @param base_port: The starting port number we are trying to get our nodes to listen on.
        @param xmx: Java memory parameter.
        @param output_dir: The directory where we can create an output file for this process.
        @return: The node object.
        """
        self.cloud_num = cloud_num
        self.nodes_per_cloud = nodes_per_cloud
        self.node_num = node_num
        self.cloud_name = cloud_name
        self.h2o_jar = h2o_jar
        self.base_port = base_port
        self.xmx = xmx
        self.output_dir = output_dir

        self.port = -1
        self.pid = -1
        self.output_file_name = ""
        self.child = None
        self.terminated = False

        # Choose my base port number here.  All math is done here.  Every node has the same
        # base_port and calculates it's own my_base_port.
        ports_per_node = 2
        self.my_base_port = \
            self.base_port + \
            (self.cloud_num * self.nodes_per_cloud * ports_per_node) + \
            (self.node_num * ports_per_node)

    def start(self):
        """
        Start one node of H2O.
        (Stash away the self.child and self.pid internally here.)

        @return: none
        """
        cmd = ["java",
               "-Xmx" + self.xmx,
               "-ea",
               "-jar", self.h2o_jar,
               "-name", self.cloud_name,
               "-base_port", str(self.my_base_port)]
        self.output_file_name = \
            os.path.join(self.output_dir, "java_" + str(self.cloud_num) + "_" + str(self.node_num) + ".out")
        f = open(self.output_file_name, "w")
        self.child = subprocess.Popen(args=cmd,
                                      stdout=f,
                                      stderr=subprocess.STDOUT,
                                      cwd=self.output_dir)
        self.pid = self.child.pid
        print("+ CMD: " + ' '.join(cmd))

    def scrape_port_from_stdout(self):
        """
        Look at the stdout log and figure out which port the JVM chose.
        Write this to self.port.
        This call is blocking.
        Exit if this fails.

        @return: none
        """
        retries = 30
        while (retries > 0):
            if (self.terminated):
                return
            f = open(self.output_file_name, "r")
            s = f.readline()
            while (len(s) > 0):
                if (self.terminated):
                    return
                match_groups = re.search(r"Listening for HTTP and REST traffic on  http://(\S+):(\d+)", s)
                if (match_groups is not None):
                    port = match_groups.group(2)
                    if (port is not None):
                        self.port = port
                        f.close()
                        print("H2O Cloud {} Node {} started with output file {}".format(self.cloud_num,
                                                                                        self.node_num,
                                                                                        self.output_file_name))
                        return

                s = f.readline()

            f.close()
            retries -= 1
            if (self.terminated):
                return
            time.sleep(1)

        print("ERROR: Too many retries starting cloud.")
        sys.exit(1)

    def stop(self):
        """
        Normal node shutdown.
        Ignore failures for now.

        @return: none
        """
        if (self.pid > 0):
            print("Killing JVM with PID {}".format(self.pid))
            try:
                self.child.terminate()
            except OSError:
                pass
            self.pid = -1

    def terminate(self):
        """
        Terminate a running node.  (Due to a signal.)

        @return: none
        """
        self.terminated = True
        self.stop()

    def get_port(self):
        """ Return the port this node is really listening on. """
        return self.port

    def __str__(self):
        s = ""
        s += "    node {}\n".format(self.node_num)
        s += "        xmx:          {}\n".format(self.xmx)
        s += "        my_base_port: {}\n".format(self.my_base_port)
        s += "        port:         {}\n".format(self.port)
        s += "        pid:          {}\n".format(self.pid)
        return s


class H2OCloud:
    """
    A class representing one of the H2O clouds.
    """

    def __init__(self, cloud_num, nodes_per_cloud, h2o_jar, base_port, xmx, output_dir):
        """
        Create a cloud.
        See node definition above for argument descriptions.

        @return: The cloud object.
        """
        self.cloud_num = cloud_num
        self.nodes_per_cloud = nodes_per_cloud
        self.h2o_jar = h2o_jar
        self.base_port = base_port
        self.xmx = xmx
        self.output_dir = output_dir

        # Randomly choose a five digit cloud number.
        n = random.randint(10000, 99999)
        user = getpass.getuser()
        user = ''.join(user.split())

        self.cloud_name = "H2O_runit_{}_{}".format(user, n)
        self.nodes = []
        self.jobs_run = 0

        for node_num in range(self.nodes_per_cloud):
            node = H2OCloudNode(self.cloud_num, self.nodes_per_cloud, node_num, self.cloud_name,
                                self.h2o_jar, self.base_port, self.xmx, self.output_dir)
            self.nodes.append(node)

    def start(self):
        """
        Start H2O cloud.
        The cloud is not up until wait_for_cloud_to_be_up() is called and returns.

        @return: none
        """
        if (self.nodes_per_cloud > 1):
            print("ERROR: Unimplemented wait for cloud size > 1")
            sys.exit(1)

        for node in self.nodes:
            node.start()

    def wait_for_cloud_to_be_up(self):
        """
        Blocking call ensuring the cloud is available.

        @return: none
        """
        self._scrape_port_from_stdout()

    def stop(self):
        """
        Normal cloud shutdown.

        @return: none
        """
        for node in self.nodes:
            node.stop()

    def terminate(self):
        """
        Terminate a running cloud.  (Due to a signal.)

        @return: none
        """
        for node in self.nodes:
            node.terminate()

    def get_port(self):
        """ Return a port to use to talk to this cloud. """
        node = self.nodes[0]
        return node.get_port()

    def _scrape_port_from_stdout(self):
        for node in self.nodes:
            node.scrape_port_from_stdout()

    def __str__(self):
        s = ""
        s += "cloud {}\n".format(self.cloud_num)
        s += "    name:     {}\n".format(self.cloud_name)
        s += "    jobs_run: {}\n".format(self.jobs_run)
        for node in self.nodes:
            s += str(node)
        return s


class Test:
    """
    A class representing one Test.

    cancelled: Don't start this test.
    terminated: Test killed due to signal.
    returncode: Exit code of child.
    pid: Process id of the test.
    ip: IP of cloud to run test.
    port: Port of cloud to run test.
    child: subprocess.Popen object.
    """

    @staticmethod
    def test_did_not_complete():
        """
        returncode marker to know if the test ran or not.
        """
        return -9999999

    def __init__(self, test_dir, test_short_dir, test_name, output_dir):
        """
        Create a Test.

        @param test_dir: Full absolute path to the test directory.
        @param test_short_dir: Path from h2o/R/tests to the test directory.
        @param test_name: Test filename with the directory removed.
        @param output_dir: The directory where we can create an output file for this process.
        @return: The test object.
        """
        self.test_dir = test_dir
        self.test_short_dir = test_short_dir
        self.test_name = test_name
        self.output_dir = output_dir
        self.output_file_name = ""

        self.cancelled = False
        self.terminated = False
        self.returncode = Test.test_did_not_complete()
        self.pid = -1
        self.ip = None
        self.port = -1
        self.child = None

    def start(self, ip, port):
        """
        Start the test in a non-blocking fashion.

        @param ip: IP address of cloud to run on.
        @param port: Port of cloud to run on.
        @return: none
        """
        if (self.cancelled or self.terminated):
            return

        self.ip = ip
        self.port = port

        cmd = ["R",
               "-f",
               self.test_name,
               "--args",
               self.ip + ":" + str(self.port)]
        test_short_dir_with_no_slashes = re.sub(r'[\\/]', "_", self.test_short_dir)
        self.output_file_name = \
            os.path.join(self.output_dir, test_short_dir_with_no_slashes + "_" + self.test_name + ".out")
        f = open(self.output_file_name, "w")
        self.child = subprocess.Popen(args=cmd,
                                      stdout=f,
                                      stderr=subprocess.STDOUT,
                                      cwd=self.test_dir)
        self.pid = self.child.pid
        # print("+ CMD: " + ' '.join(cmd))

    def is_completed(self):
        """
        Check if test has completed.

        This has side effects and MUST be called for the normal test queueing to work.
        Specifically, child.poll().

        @return: True if the test completed, False otherwise.
        """
        child = self.child
        if (child is None):
            return False
        child.poll()
        if (child.returncode is None):
            return False
        self.pid = -1
        self.returncode = child.returncode
        return True

    def cancel(self):
        """
        Mark this test as cancelled so it never tries to start.

        @return: none
        """
        if (self.pid <= 0):
            self.cancelled = True

    def terminate(self):
        """
        Terminate a running test.  (Due to a signal.)

        @return: none
        """
        self.terminated = True
        if (self.pid > 0):
            print("Killing Test with PID {}".format(self.pid))
            try:
                self.child.terminate()
            except OSError:
                pass
        self.pid = -1

    def get_test_dir_file_name(self):
        """
        @return: The full absolute path of this test.
        """
        return os.path.join(self.test_dir, self.test_name)

    def get_test_name(self):
        """
        @return: The file name (no directory) of this test.
        """
        return self.test_name

    def get_port(self):
        """
        @return: Integer port number of the cloud where this test ran.
        """
        return int(self.port)

    def get_passed(self):
        """
        @return: True if the test passed, False otherwise.
        """
        return (self.returncode == 0)

    def get_completed(self):
        """
        @return: True if the test completed (pass or fail), False otherwise.
        """
        return (self.returncode > Test.test_did_not_complete())

    def get_output_dir_file_name(self):
        """
        @return: Full path to the output file which you can paste to a terminal window.
        """
        return (os.path.join(self.output_dir, self.output_file_name))

    def __str__(self):
        s = ""
        s += "Test: {}/{}\n".format(self.test_dir, self.test_name)
        return s


class RUnitRunner:
    """
    A class for running the RUnit tests.

    The tests list contains an object for every test.
    The tests_not_started list acts as a job queue.
    The tests_running list is polled for jobs that have finished.
    """

    def __init__(self, test_root_dir, num_clouds, nodes_per_cloud, h2o_jar, base_port, xmx, output_dir):
        """
        Create a runner.

        @param test_root_dir: h2o/R/tests directory.
        @param num_clouds: Number of H2O clouds to start.
        @param nodes_per_cloud: Number of H2O nodes to start per cloud.
        @param h2o_jar: Path to H2O jar file to run.
        @param base_port: Base H2O port (e.g. 54321) to start choosing from.
        @param xmx: Java -Xmx parameter.
        @param output_dir: Directory for output files.
        @return: The runner object.
        """
        self.test_root_dir = test_root_dir
        self.num_clouds = num_clouds
        self.nodes_per_cloud = nodes_per_cloud
        self.h2o_jar = h2o_jar
        self.base_port = base_port
        self.output_dir = output_dir

        self.terminated = False
        self.clouds = []
        self.tests = []
        self.tests_not_started = []
        self.tests_running = []
        self._create_output_dir()

        for i in range(self.num_clouds):
            cloud = H2OCloud(i, self.nodes_per_cloud, h2o_jar, self.base_port, xmx, self.output_dir)
            self.clouds.append(cloud)

    def read_test_list_file(self, test_list_file):
        """
        Read in a test list file line by line.  Each line in the file is a test
        to add to the test run.

        @param test_list_file: Filesystem path to a file with a list of tests to run.
        @return: none
        """
        try:
            f = open(test_list_file, "r")
            s = f.readline()
            while (len(s) != 0):
                stripped = s.strip()
                if (len(stripped) == 0):
                    s = f.readline()
                    continue
                if (stripped.startswith("#")):
                    s = f.readline()
                    continue
                self.add_test(stripped)
                s = f.readline()
            f.close()
        except IOError as e:
            print("")
            print("ERROR: Failure reading test list (" + test_list_file + ")")
            print("       (errno {0}): {1}".format(e.errno, e.strerror))
            print("")
            sys.exit(1)

    def build_test_list(self):
        """
        Recursively find the list of tests to run and store them in the object.
        Fills in self.tests and self.tests_not_started.

        @return:  none
        """
        if (self.terminated):
            return

        for root, dirs, files in os.walk(self.test_root_dir):
            if (root.endswith("Util")):
                continue

            for f in files:
                if (not re.match(".*runit.*\.[rR]", f)):
                    continue
                self.add_test(os.path.join(root, f))

    def add_test(self, test_path):
        """
        Add one test to the list of tests to run.

        @param test_path: File system path to the test.
        @return: none
        """
        abs_test_root_dir = os.path.abspath(self.test_root_dir)
        abs_test_path = os.path.abspath(test_path)
        abs_test_dir = os.path.dirname(abs_test_path)
        test_file = os.path.basename(abs_test_path)

        if (not os.path.exists(abs_test_path)):
            print("")
            print("ERROR: Test does not exist (" + abs_test_path + ")")
            print("")
            sys.exit(1)

        test_short_dir = abs_test_dir
        prefix = os.path.join(abs_test_root_dir, "")
        if (test_short_dir.startswith(prefix)):
            test_short_dir = test_short_dir.replace(prefix, "", 1)

        test = Test(abs_test_dir, test_short_dir, test_file, self.output_dir)
        self.tests.append(test)
        self.tests_not_started.append(test)

    def start_clouds(self):
        """
        Start all H2O clouds.

        @return: none
        """
        if (self.terminated):
            return

        print("")
        print("Starting clouds...")
        print("")

        for cloud in self.clouds:
            if (self.terminated):
                return
            cloud.start()

        print("")
        print("Waiting for H2O nodes to come up...")
        print("")

        for cloud in self.clouds:
            if (self.terminated):
                return
            cloud.wait_for_cloud_to_be_up()

    def run_tests(self):
        """
        Run all tests.

        @return: none
        """
        if (self.terminated):
            return

        num_tests = len(self.tests)
        num_nodes = len(self.clouds * self.nodes_per_cloud)
        self._log("")
        self._log("Starting {} tests on {} H2O nodes...".format(num_tests, num_nodes))
        self._log("")

        # Start the first n tests, where n is the lesser of the total number of tests and the total number of clouds.
        start_count = min(len(self.tests_not_started), len(self.clouds))
        for i in range(start_count):
            cloud = self.clouds[i]
            port = cloud.get_port()
            self._start_next_test_on_port(port)

        # As each test finishes, send a new one to the cloud that just freed up.
        while (len(self.tests_not_started) > 0):
            if (self.terminated):
                return
            completed_test = self._wait_for_one_test_to_complete()
            if (self.terminated):
                return
            self._report_test_result(completed_test)
            port_of_completed_test = completed_test.get_port()
            self._start_next_test_on_port(port_of_completed_test)

        # Wait for remaining running tests to complete.
        while (len(self.tests_running) > 0):
            if (self.terminated):
                return
            completed_test = self._wait_for_one_test_to_complete()
            if (self.terminated):
                return
            self._report_test_result(completed_test)

    def stop_clouds(self):
        """
        Stop all H2O clouds.

        @return: none
        """
        if (self.terminated):
            return
        print("")
        print("All tests completed; tearing down clouds...")
        print("")
        for cloud in self.clouds:
            cloud.stop()

    def report_summary(self):
        """
        Report some summary information when the tests have finished running.

        @return: none
        """
        passed = 0
        failed = 0
        notrun = 0
        total = 0
        for test in self.tests:
            if (test.get_passed()):
                passed += 1
            else:
                if (test.get_completed()):
                    failed += 1
                else:
                    notrun += 1
            total += 1
        self._log("")
        self._log("----------------------------------------------------------------------")
        self._log("")
        self._log("SUMMARY OF RESULTS")
        self._log("")
        self._log("----------------------------------------------------------------------")
        self._log("")
        self._log("Total tests:      " + str(total))
        self._log("Passed:           " + str(passed))
        self._log("Did not pass:     " + str(failed))
        self._log("Did not complete: " + str(notrun))
        self._log("")

    def terminate(self):
        """
        Terminate all running clouds.  (Due to a signal.)

        @return: none
        """
        self.terminated = True

        for test in self.tests:
            test.cancel()

        for test in self.tests:
            test.terminate()

        for cloud in self.clouds:
            cloud.terminate()

    #--------------------------------------------------------------------
    # Private methods below this line.
    #--------------------------------------------------------------------

    def _create_output_dir(self):
        try:
            os.makedirs(self.output_dir)
        except OSError as e:
            print("")
            print("mkdir failed (errno {0}): {1}".format(e.errno, e.strerror))
            print("    " + self.output_dir)
            print("")
            print("(try adding --wipe)")
            print("")
            sys.exit(1)

    def _start_next_test_on_port(self, port):
        test = self.tests_not_started.pop(0)
        self.tests_running.append(test)
        ip = "127.0.0.1"
        test.start(ip, port)

    def _wait_for_one_test_to_complete(self):
        while (True):
            for test in self.tests_running:
                if (self.terminated):
                    return None
                if (test.is_completed()):
                    self.tests_running.remove(test)
                    return test
            if (self.terminated):
                return
            time.sleep(1)

    def _report_test_result(self, test):
        port = test.get_port()
        if (test.get_passed()):
            s = "PASS      %d %-70s" % (port, test.get_test_name())
            self._log(s)
        else:
            s = "     FAIL %d %-70s %s" % (port, test.get_test_name(), test.get_output_dir_file_name())
            self._log(s)
            f = self._get_failed_filehandle_for_appending()
            f.write(test.get_test_dir_file_name() + "\n")
            f.close()

    def _log(self, s):
        f = self._get_summary_filehandle_for_appending()
        print(s)
        f.write(s + "\n")
        f.close()

    def _get_summary_filehandle_for_appending(self):
        summary_file_name = os.path.join(self.output_dir, "summary.txt")
        f = open(summary_file_name, "a")
        return f

    def _get_failed_filehandle_for_appending(self):
        summary_file_name = os.path.join(self.output_dir, "failed.txt")
        f = open(summary_file_name, "a")
        return f

    def __str__(self):
        s = "\n"
        s += "test_root_dir:    {}\n".format(self.test_root_dir)
        s += "output_dir:       {}\n".format(self.output_dir)
        s += "h2o_jar:          {}\n".format(self.h2o_jar)
        s += "num_clouds:       {}\n".format(self.num_clouds)
        s += "nodes_per_cloud:  {}\n".format(self.nodes_per_cloud)
        s += "base_port:        {}\n".format(self.base_port)
        s += "\n"
        for c in self.clouds:
            s += str(c)
        s += "\n"
        # for t in self.tests:
        #     s += str(t)
        return s


#--------------------------------------------------------------------
# Main program
#--------------------------------------------------------------------

# Global variables that can be set by the user.
g_base_port = 40000
g_num_clouds = 3
g_wipe_output_dir = False
g_test_to_run = None
g_test_list_file = None

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
    print("usage:  $0"
          " [--wipe]"
          " [--baseport port]"
          " [--numclouds n]"
          " [--test path/to/test.R]"
          " [--testlist path/to/list/file]")
    print("")
    print("    (Output dir is: " + g_output_dir + ")")
    print("")
    print("    --wipe        Wipes the output dir before starting.")
    print("    --baseport    The port at which H2O starts searching for ports")
    print("    --numclouds   The number of clouds to start.")
    print("                  Each test is randomly assigned to a cloud.")
    print("    --test        If you only want to run one test, specify it like this.")
    print("    --testlist    A file containing a list of tests to run (for example the")
    print("                  'failed.txt' file from the output directory).")
    print("")
    sys.exit(1)


def parse_args(argv):
    global g_base_port
    global g_num_clouds
    global g_wipe_output_dir
    global g_test_to_run
    global g_test_list_file

    i = 1
    while (i < len(argv)):
        s = argv[i]

        if (s == "--baseport"):
            i += 1
            if (i > len(argv)):
                usage()
            g_base_port = int(argv[i])
        elif (s == "--numclouds"):
            i += 1
            if (i > len(argv)):
                usage()
            g_num_clouds = int(argv[i])
        elif (s == "--wipe"):
            g_wipe_output_dir = True
        elif (s == "--test"):
            i += 1
            if (i > len(argv)):
                usage()
            g_test_to_run = argv[i]
        elif (s == "--testlist"):
            i += 1
            if (i > len(argv)):
                usage()
            g_test_list_file = argv[i]
        else:
            usage()

        i += 1


def main(argv):
    """
    Main program.

    @return: none
    """
    global g_output_dir
    global g_test_to_run
    global g_test_list_file
    global g_runner

    test_root_dir = os.path.dirname(os.path.realpath(__file__))

    # Calculate global variables.
    g_output_dir = os.path.join(test_root_dir, "results")

    # Calculate and set other variables.
    nodes_per_cloud = 1
    xmx = "2g"
    h2o_jar = os.path.abspath(
        os.path.join(os.path.join(os.path.join(os.path.join(
            test_root_dir, ".."), ".."), "target"), "h2o.jar"))

    # Override any defaults with the user's choices.
    parse_args(argv)

    # Set up output directory.
    if (not os.path.exists(h2o_jar)):
        print("")
        print("H2O jar not found: {}".format(h2o_jar))
        print("    " + g_output_dir)
        print("")
        sys.exit(1)

    if (g_wipe_output_dir):
        try:
            if (os.path.exists(g_output_dir)):
                shutil.rmtree(g_output_dir)
        except OSError as e:
            print("")
            print("Removing directory failed (errno {0}): {1}".format(e.errno, e.strerror))
            print("    " + g_output_dir)
            print("")
            sys.exit(1)

    # Create runner object.
    g_runner = RUnitRunner(test_root_dir, g_num_clouds, nodes_per_cloud, h2o_jar, g_base_port, xmx, g_output_dir)

    # Build test list.
    if (g_test_list_file is not None):
        g_runner.read_test_list_file(g_test_list_file)
    elif (g_test_to_run is not None):
        g_runner.add_test(g_test_to_run)
    else:
        g_runner.build_test_list()

    # Handle killing the runner.
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run.
    g_runner.start_clouds()
    g_runner.run_tests()
    g_runner.stop_clouds()
    g_runner.report_summary()


if __name__ == "__main__":
    main(sys.argv)
