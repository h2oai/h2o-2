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

    port: The actual port chosen at run time.
    pid: The process id of the node.
    """
    def __init__(self, cloud_num, nodes_per_cloud, node_num, cloud_name, h2o_jar, base_port, xmx, output_dir):
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

        ports_per_node = 2
        self.my_base_port = \
            self.base_port + \
            (self.cloud_num * self.nodes_per_cloud * ports_per_node) + \
            (self.node_num * ports_per_node)

    def start(self):
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
        Look at the stdout log and try to
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
        if (self.pid > 0):
            print("Killing JVM with PID {}".format(self.pid))
            os.kill(self.pid, signal.SIGTERM)

    def terminate(self):
        self.terminated = True
        self.stop()

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
        self.cloud_num = cloud_num
        self.nodes_per_cloud = nodes_per_cloud
        self.h2o_jar = h2o_jar
        self.base_port = base_port
        self.xmx = xmx
        self.output_dir = output_dir

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
        if (self.nodes_per_cloud > 1):
            print("ERROR: Unimplemented wait for cloud size > 1")
            sys.exit(1)

        for node in self.nodes:
            node.start()

    def scrape_port_from_stdout(self):
        for node in self.nodes:
            node.scrape_port_from_stdout()

    def stop(self):
        for node in self.nodes:
            node.stop()

    def terminate(self):
        for node in self.nodes:
            node.terminate()

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
    """

    def __init__(self, test_dir, test_name, output_dir):
        self.test_dir = test_dir
        self.test_name = test_name
        self.output_dir = output_dir

        self.terminated = False
        self.exit_status = -9999
        self.pid = -1

    def start(self):
        if (self.terminated):
            return

    def terminate(self):
        self.terminated = True
        if (self.pid > 0):
            print("Killing Test with PID {}".format(self.pid))
            os.kill(self.pid, signal.SIGTERM)
            self.pid = -1

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
        Constructor.

        @param test_root_dir:
        @param num_clouds: Number of H2O clouds to start.
        @param nodes_per_cloud: Number of H2O nodes to start per cloud.
        @param h2o_jar: Path to H2O jar file to run.
        @param base_port: Base H2O port (e.g. 54321) to start choosing from.
        @param xmx: Java -Xmx parameter.
        @param output_dir: Directory for output files.
        @return: The object.
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
        self.create_output_dir()

        for i in range(self.num_clouds):
            cloud = H2OCloud(i, self.nodes_per_cloud, h2o_jar, self.base_port, xmx, self.output_dir)
            self.clouds.append(cloud)

    def build_test_list(self):
        """
        Recursively find the list of tests to run and store them in the object.
        @return:  none
        """
        if (self.terminated):
            return

        for root, dirs, files in os.walk(self.test_root_dir):
            if (root.endswith("Util")):
                continue

            for f in files:
                if (not re.search("runit", f)):
                    continue

                test = Test(root, f, self.output_dir)
                self.tests.append(test)
                self.tests_not_started.append(test)

    def start_clouds(self):
        if (self.terminated):
            return
        for cloud in self.clouds:
            if (self.terminated):
                return
            cloud.start()

        print("Waiting for H2O nodes to come up...")

        for cloud in self.clouds:
            if (self.terminated):
                return
            cloud.scrape_port_from_stdout()

    def run_tests(self):
        if (self.terminated):
            return

    def stop_clouds(self):
        if (self.terminated):
            return
        for cloud in self.clouds:
            cloud.stop()

    def terminate(self):
        self.terminated = True

        for test in self.tests:
            test.terminate()

        for cloud in self.clouds:
            cloud.terminate()

    #--------------------------------------------------------------------
    # Private methods below this line.
    #--------------------------------------------------------------------

    def create_output_dir(self):
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
base_port = 40000
num_clouds = 3

# Global variables that are set internally.
output_dir = None
wipe_output_dir = False
runner = None
handling_signal = False


def signal_handler(signum, stackframe):
    global runner
    global handling_signal

    if (handling_signal):
        # Don't do this recursively.
        return
    handling_signal = True

    print("")
    print("----------------------------------------------------------------------")
    print("")
    print("SIGNAL CAUGHT.  SHUTTING DOWN NOW.")
    print("")
    print("----------------------------------------------------------------------")
    runner.terminate()


def usage():
    print("")
    print("usage:  $0 [--baseport port] [--numclouds n] [--wipe]")
    print("")
    print("    --wipe wipes the output dir before starting")
    print("    (Output dir is: " + output_dir + ")")
    print("")
    sys.exit(1)


def parse_args(argv):
    global base_port
    global num_clouds
    global wipe_output_dir

    i = 1
    while (i < len(argv)):
        s = argv[i]

        if (s == "--baseport"):
            i += 1
            if (i > len(argv)):
                usage()
            base_port = argv[i]
        elif (s == "--numclouds"):
            i += 1
            if (i > len(argv)):
                usage()
            num_clouds = argv[i]
        elif (s == "--wipe"):
            wipe_output_dir = True
        else:
            usage()

        i += 1


def main(argv):
    global output_dir
    global runner

    # Calculate global variables.
    test_root_dir = os.path.dirname(os.path.realpath(__file__))
    output_dir = test_root_dir + "/" + "results"

    # Calculate and set other variables.
    nodes_per_cloud = 1
    xmx = "2g"
    h2o_jar = os.path.abspath(test_root_dir + "/../../target/h2o.jar")

    # Override any defaults with the user's choices.
    parse_args(argv)

    # Set up output directory.
    if (not os.path.exists(h2o_jar)):
        print("")
        print("h2o jar not found: {}".format(h2o_jar))
        print("    " + output_dir)
        print("")
        sys.exit(1)

    if (wipe_output_dir):
        try:
            if (os.path.exists(output_dir)):
                shutil.rmtree(output_dir)
        except OSError as e:
            print("")
            print("removing directory failed (errno {0}): {1}".format(e.errno, e.strerror))
            print("    " + output_dir)
            print("")
            sys.exit(1)

    # Create runner object.
    runner = RUnitRunner(test_root_dir, num_clouds, nodes_per_cloud, h2o_jar, base_port, xmx, output_dir)

    # Handle killing the runner.
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run.
    runner.build_test_list()
    runner.start_clouds()
    runner.run_tests()

    # print str(runner)

    if (not runner.terminated):
        time.sleep(100)

    runner.stop_clouds()


if __name__ == "__main__":
    main(sys.argv)
