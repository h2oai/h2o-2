class PerfRunner:
    """
    A class for running the perfomrance tests.

    The tests list contains a Test object for every test.
    The tests_not_started is a queue.
    Each test blocks until it completes.
    """
    def __init__(self, test_root_dir, output_dir, nodes_in_cloud,
                 xmx, h2o_jar, use_cloud = False, use_ip = "", use_port = "", base_port = 40000):
        """
        Create runner.

        @param test_root_dir: h2o/bench2/tests
        @param output_dir: Directory of output files
        @param nodes_in_cloud: Number of H2O nodes to start
        @param xmx: Java -Xmx parameter
        @param use_cloud: Use this user-specified cloud
        @param use_ip: if use_cloud: use_ip
        @param use_port: if use_cloud: use_port
        @param base_port: Base H2O port (e.g. 54321) to start choosing from.
        """
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

        #self.start_epoch_ms = time.time()
        self.terminated = False
        self.cloud = ""
        self.tests = []
        self.tests_not_started = []
        self.tests_running = []
        self.__create_output_dir__()

        if use_cloud:
            cloud = H2OUseCloud(0, use_ip, use_port)
            self.cloud = cloud
        else:
            cloud = H2OCloud(1, self.nodes_in_cloud, h2o_jar, self.base_port, xmx, self.output_dir)
            self.cloud = cloud

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
        parse_file = testDir + "_Parse.R"
        model_file = testDir + "_Model.R"
        predict_file = testDir + "_Predict.R"
        test_dir = os.path.join(self.test_root_dir, testDir)
        test_short_dir = testDir

        test = Test(-1, -1, test_dir, test_short_dir, 
                    self.output_dir, parse_file, model_file, predict_file)

        self.tests.append(test)
        self.tests_not_started.append(test)

    def start_cloud(self):
        """
        Start H2O Cloud
        """
        if (self.terminated):
            return

        if (self.use_cloud):
            return

        print("")
        print("Starting cloud...")
        print("")

        if (self.terminated):
            return
        self.cloud.start()

        print("")
        print("Waiting for H2O nodes to come up...")
        print("")

        if (self.terminated):
            return
        self.cloud.wait_for_cloud_to_be_up()
        self.jvm_output_file = self.cloud.nodes[0].get_output_file_name()

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
            self.start_cloud()
            test = self.tests_not_started.pop(0)
            test.ip = self.cloud.get_ip()
            test.port = self.cloud.get_port()
            test.test_run = TableRow("test_run")
            test.test_run.row.update(self.__scrape_h2o_sys_info__())
            test.do_test()
            test.test_run.row['start_epoch_ms'] = test.start_ms
            test.test_run.row['end_epoch_ms'] = test.end_ms
            test.test_run.row['total_hosts'] = self.__get_num_hosts__()
            test.test_run.row['total_nodes'] = self.__get_num_nodes__()
            test.test_run.row['test_name'] = test.test_name
            test.test_run.row['instance_type'] = self.__get_instance_type__()
            test.test_run.update(True)
            self.stop_cloud()

    def __get_instance_type__(self):
        return "localhost"

    def __get_num_hosts__(self):
        num_hosts = 0
        for node in self.cloud.nodes:
            num_hosts += 1 #len(node)
        return num_hosts

    def __get_num_nodes__(self):
        return len(self.cloud.nodes)

    def __scrape_h2o_sys_info__(self):
        """
        Scrapes the following information from the jvm_output_file:
            user_name, build_version, build_branch, build_sha, build_date,
            cpus_per_host, heap_bytes_per_node
        """
        test_run_dict = {}
        test_run_dict['product_name'] = "h2o"
        test_run_dict['component_name'] = "None"
        with open(self.jvm_output_file, "r") as f:
            for line in f:
              line = line.replace('\n', '')
              if "Built by" in line:
                  test_run_dict['user_name'] = line.split(': ')[-1]
              if "Build git branch" in line:
                  test_run_dict['build_branch'] = line.split(': ')[-1]
              if "Build git hash" in line:
                  test_run_dict['build_sha'] = line.split(': ')[-1]
              if "Build project version" in line:
                  test_run_dict['build_version'] = line.split(': ')[-1]
              if "Built on" in line:
                  test_run_dict['build_date'] = line.split(': ')[-1]
              if "Java availableProcessors" in line:
                  test_run_dict['cpus_per_host'] = line.split(': ')[-1]
              if "Java heap maxMemory" in line:
                  test_run_dict['heap_bytes_per_node'] = str(float(line.split(': ')[-1].split(' ')[0]) * 1024 * 1024)
              if "error" in line.lower():
                  test_run_dict['error_message'] = line
              else:
                  test_run_dict['error_message'] = "No error"
        return test_run_dict

    def __log__(self, s):
        f = self._get_summary_filehandle_for_appending()
        print(s)
        f.write(s + "\n")
        f.close()

    def stop_cloud(self):
        """
        Stop H2O cloud.
        """
        if self.terminated:
            return
      
        if self.use_cloud:
            print("")
            print("All tests completed...")
            print("")
            return

        print("")
        print("All tets completed; tearing down clouds...")
        print("")
        self.cloud.stop()

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
        end_seconds = time.time()
        delta_seconds = end_seconds - self.start_seconds
        run = total - notrun
        self.__log__("")
        self.__log__("----------------------------------------------------------------------")
        self.__log__("")
        self.__log__("SUMMARY OF RESULTS")
        self.__log__("")
        self.__log__("----------------------------------------------------------------------")
        self.__log__("")
        self.__log__("Total tests:          " + str(total))
        self.__log__("Passed:               " + str(passed))
        self.__log__("Did not pass:         " + str(failed))
        self.__log__("Did not complete:     " + str(notrun))
        self.__log__("")
        self.__log__("Total time:           %.2f sec" % delta_seconds)
        if (run > 0):
            self.__log__("Time/completed test:  %.2f sec" % (delta_seconds / run))
        else:
            self.__log__("Time/completed test:  N/A")
        self.__log__("")

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

