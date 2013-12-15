#!/usr/bin/python

import sys
import os
import shutil


class H2OCloudNode:
    """
    A class representing one node in an H2O cloud.

    port: The actual port chosen at run time.
    pid: The process id of the node.
    """
    def __init__(self, cloud_num, node_num, base_port, xmx, output_dir):
        self.cloud_num = cloud_num
        self.node_num = node_num
        self.base_port = base_port
        self.xmx = xmx
        self.output_dir = output_dir

        self.port = -1
        self.pid = -1

    def start(self):
        pass

    def stop(self):
        pass

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
    def __init__(self, cloud_num, nodes_per_cloud, base_port, xmx, output_dir):
        self.cloud_num = cloud_num
        self.nodes_per_cloud = nodes_per_cloud
        self.base_port = base_port
        self.xmx = xmx
        self.output_dir = output_dir

        self.nodes = []
        self.jobs_run = 0

        for i in range(self.nodes_per_cloud):
            node = H2OCloudNode(self.cloud_num, i, self.base_port, self.xmx, self.output_dir)
            self.nodes.append(node)

    def start(self):
        pass

    def stop(self):
        pass

    def __str__(self):
        s = ""
        s += "cloud {}\n".format(self.cloud_num)
        s += "    jobs_run: {}\n".format(self.jobs_run)
        for node in self.nodes:
            s += str(node)
        return s


class RUnitRunner:
    """
    A class for running the RUnit tests.
    """

    def __init__(self, test_root_dir, num_clouds, nodes_per_cloud, base_port, xmx, output_dir):
        """
        Constructor.

        @param test_root_dir:
        @param num_clouds: Number of H2O clouds to start.
        @param nodes_per_cloud: Number of H2O nodes to start per cloud.
        @param base_port: Base H2O port (e.g. 54321) to start choosing from.
        @param xmx: Java -Xmx parameter.
        @param output_dir: Directory for output files.
        @return: The object.
        """
        self.test_root_dir = test_root_dir
        self.num_clouds = num_clouds
        self.nodes_per_cloud = nodes_per_cloud
        self.base_port = base_port
        self.output_dir = output_dir

        self.clouds = []
        self.tests = []

        for i in range(self.num_clouds):
            cloud = H2OCloud(i, self.nodes_per_cloud, self.base_port, xmx, self.output_dir)
            self.clouds.append(cloud)

    def run(self):
        """
        Do the work.

        @return: none
        """
        self.create_output_dir()
        self.build_test_list()
        self.start_clouds()
        self.run_tests()
        self.stop_clouds()

    #--------------------------------------------------------------------
    # Private methods below this line.
    #--------------------------------------------------------------------

    def create_output_dir(self):
        try:
            os.mkdir(self.output_dir)
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
    test_root_dir = os.path.dirname(os.path.realpath(__file__))
    output_dir = test_root_dir + "/" + "results"

    parse_args(argv)

    if (wipe_output_dir):
        try:
            shutil.rmtree(output_dir)
        except OSError as e:
            print("")
            print("removing directory failed (errno {0}): {1}".format(e.errno, e.strerror))
            print("    " + output_dir)
            print("")
            sys.exit(1)

    nodes_per_cloud = 1
    xmx = "2g"
    runner = RUnitRunner(test_root_dir, num_clouds, nodes_per_cloud, base_port, xmx, output_dir)
    runner.run()
    print str(runner)


if __name__ == "__main__":
    main(sys.argv)
