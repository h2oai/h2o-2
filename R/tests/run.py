#!/usr/bin/python

import sys
import os
import shutil
import signal
import time


class H2OCloudNode:
    """
    A class representing one node in an H2O cloud.

    port: The actual port chosen at run time.
    pid: The process id of the node.
    """
    def __init__(self, cloud_num, node_num, h2o_jar, base_port, xmx, output_dir):
        self.cloud_num = cloud_num
        self.node_num = node_num
        self.h2o_jar = h2o_jar
        self.base_port = base_port
        self.xmx = xmx
        self.output_dir = output_dir

        self.terminated = False
        self.port = -1
        self.pid = -1

    def start(self):
        pass

    def stop(self):
        pass

    def terminate(self):
        self.terminated = True
        if (self.pid > 0):
            print("Killing JVM with PID {}".format(self.pid))
            os.kill(self.pid)

    def __str__(self):
        s = ""
        s += "    cloud {} code {}\n".format(self.cloud_num, self.node_num)
        s += "        xmx:  {}\n".format(self.xmx)
        s += "        port: {}\n".format(self.port)
        s += "        pid:  {}\n".format(self.pid)
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

        self.terminated = False
        self.nodes = []
        self.jobs_run = 0

        for i in range(self.nodes_per_cloud):
            node = H2OCloudNode(self.cloud_num, i, h2o_jar, self.base_port, self.xmx, self.output_dir)
            self.nodes.append(node)

    def start(self):
        pass

    def stop(self):
        pass

    def terminate(self):
        self.terminated = True
        for node in self.nodes:
            node.terminate()

    def __str__(self):
        s = ""
        s += "cloud {}\n".format(self.cloud_num)
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
        self.running = False
        self.exit_status = -9999
        self.pid = -1

    def is_running(self):
        return self.running

    def start(self):
        pass

    def terminate(self):
        if (self.pid > 0):
            print("Killing Test with PID {}".format(self.pid))
            os.kill(self.pid)


class RUnitRunner:
    """
    A class for running the RUnit tests.
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

        for i in range(self.num_clouds):
            cloud = H2OCloud(i, self.nodes_per_cloud, h2o_jar, self.base_port, xmx, self.output_dir)
            self.clouds.append(cloud)

    def run(self):
        """
        Do the work.

        @return: none
        """
        if (not self.terminated):
            self.create_output_dir()

        if (not self.terminated):
            self.build_test_list()

        if (not self.terminated):
            self.start_clouds()

        if (not self.terminated):
            self.run_tests()

        self.stop_clouds()

    def terminate(self):
        self.terminated = True

        for test in self.tests:
            if (test.is_running()):
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

    def build_test_list(self):
        """
        Recursively find the list of tests to run and store them in the object.
        @return:  none
        """
        pass

    def start_clouds(self):
        pass

    def run_tests(self):
        pass

    def stop_clouds(self):
        pass

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
        return s


#--------------------------------------------------------------------
# Main program
#--------------------------------------------------------------------

base_port = 40000
num_clouds = 3
wipe_output_dir = False
output_dir = ""
h2o_jar = ""
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
    print("    (Default output dir is: " + output_dir + ")")
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
        elif (s == "--numjvms"):
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
    global runner

    test_root_dir = os.path.dirname(os.path.realpath(__file__))
    output_dir = test_root_dir + "/" + "results"
    h2o_jar = os.path.abspath(test_root_dir + "/../../target/h2o.jar")

    parse_args(argv)

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

    nodes_per_cloud = 1
    xmx = "2g"
    runner = RUnitRunner(test_root_dir, num_clouds, nodes_per_cloud, h2o_jar, base_port, xmx, output_dir)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    runner.run()
    print str(runner)

if __name__ == "__main__":
    main(sys.argv)
    time.sleep(100)
