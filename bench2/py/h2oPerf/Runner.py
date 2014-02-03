from H2O import *
from Process import *
from Table import *
import PerfUtils

import re
import os
import time

class PerfRunner:
    """
    A class for running the perfomrance tests.

    The tests list contains a Test object for every test.
    The tests_not_started is a queue.
    Each test blocks until it completes.
    """
    def __init__(self, test_root_dir, output_dir, nodes_in_cloud,
                 xmx, h2o_jar, use_cloud = False, use_ip = "", use_port = "", base_port = 40000, perfdb):
        self.test_root_dir = test_root_dir
        self.output_dir = output_dir
        self.nodes_in_cloud = nodes_in_cloud
        self.xmx = xmx
        self.use_cloud = use_cloud
        self.use_ip = use_ip
        self.use_port = use_port
        self.base_port = base_port
        self.h2o_jar = h2o_jar
        self.start_seconds = time.time()
        self.jvm_output_file = ""
        self.perfdb = perfdb

        self.start_seconds = time.time()
        self.terminated = False
        self.cloud = []
        self.tests = []
        self.tests_not_started = []
        self.tests_running = []
        self.__create_output_dir__()

        if use_cloud:
            cloud = H2OUseCloud(0, use_ip, use_port)
            self.cloud = cloud
        else:
            cloud = H2OCloud(1, self.nodes_in_cloud, h2o_jar, self.base_port, xmx, self.output_dir)
            self.cloud.append(cloud)

    def build_test_list(self):
        """
        Recursively find the list of tests to run.
        """
        if self.terminated:
            return

        for root, dirs, files in os.walk(self.test_root_dir):
            for d in dirs:
                self.add_test(d)

    def add_test(self, testDir):
        """
        Create a Test object and push it onto the queue.
        """
        config_file = testDir + ".cfg"
        parse_file = testDir + "_Parse.R"
        model_file = testDir + "_Model.R"
        predict_file = None
        if os.path.exists(os.path.join(self.test_root_dir, testDir, testDir + "_Predict.R")):
            predict_file = testDir + "_Predict.R"

        test_dir = os.path.join(self.test_root_dir, testDir)
        test_short_dir = testDir

        test = Test(config_file, test_dir, test_short_dir, 
                    self.output_dir, parse_file, model_file, predict_file, self.perfdb)

        self.tests.append(test)
        self.tests_not_started.append(test)

    def run_tests(self):
        """
        Run all tests.

        @return: none
        """
        if (self.terminated):
            return

        num_tests = len(self.tests)
        num_nodes = self.nodes_in_cloud
        self.__log__("")
        if (self.use_cloud):
            self.__log__("Starting {} tests...".format(num_tests))
        else:
            self.__log__("Starting {} tests on {} total H2O nodes...".format(num_tests, num_nodes))
        self.__log__("")

        # Do _one_ test at a time
        while len(self.tests_not_started) > 0:
            PerfUtils.start_cloud(self)
            test = self.tests_not_started.pop(0)
            test.ip = self.cloud.get_ip()
            test.port = self.cloud.get_port()
            test.test_run = TableRow("test_run", self.perfdb)
            test.test_run.row.update(PerfUtils.__scrape_h2o_sys_info__(self))
            test.do_test()
            test.test_run.row['start_epoch_ms'] = test.start_ms
            test.test_run.row['end_epoch_ms'] = test.end_ms
            test.test_run.row['test_name'] = test.test_name
            test.test_run.update(True)
            PerfUtils.stop_cloud(self)

    def __get_instance_type__(self):
        return "localhost"

    def __get_num_hosts__(self):
        num_hosts = 0
        for node in self.cloud.nodes:
            num_hosts += 1 #len(node)
        return num_hosts

    def __get_num_nodes__(self):
        return len(self.cloud.nodes)

    def __log__(self, s):
        f = self._get_summary_filehandle_for_appending()
        print(s)
        f.write(s + "\n")
        f.close()

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

        self.cloud.terminate()

    def __create_output_dir__(self):
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

    def _get_summary_filehandle_for_appending(self):
        summary_file_name = os.path.join(self.output_dir, "summary.txt")
        f = open(summary_file_name, "a")
        return f

    def _get_failed_filehandle_for_appending(self):
        summary_file_name = os.path.join(self.output_dir, "failed.txt")
        f = open(summary_file_name, "a")
        return f

