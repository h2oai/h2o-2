from H2O import *
from Process import *
import re, os, time

class Test:
    """
    A Test object is a wrapper of the directory of R files
    "under test." There are at most 3 R files per test:
        1. Parsing
        2. Modeling
        3. Predicting

    Each file represents a phase of the test.
    """
    def __init__(self, ip, port, test_dir, test_short_dir, output_dir, parse_file, model_file, predict_file):
        self.ip = ip
        self.port = port
        self.test_dir = test_dir
        self.test_short_dir = test_short_dir
        self.output_dir = output_dir
        self.parse_file = parse_file
        self.model_file = model_file
        self.predict_file = predict_file

        self.test_is_complete = False
        self.test_cancelled = False
        self.test_terminated = False
        self.start_seconds = -1

        self.parse_process = RProc(self.test_dir, self.test_short_dir, self.output_dir, self.parse_file)
        self.model_process = RProc(self.test_dir, self.test_short_dir, self.output_dir, self.model_file)
        self.predict_process = RProc(self.test_dir, self.test_short_dir, self.output_dir, self.predict_file)

    def do_test(self):
        """
        This call is blocking.

        Perform the test. Each phase is started by passing the ip and port.
        We block until a phase completes.

        Once a phase is complete, the stdouterr is scraped and the database
        tables are updated.
        """
        self.start_seconds = time.time()
        self.parse_process.start(self.ip, self.port)
        self.parse_process.block()
        self.parse_process.scrape_phase()

        self.model_process.start(self.ip, self.port)
        self.model_process.block()
        self.model_process.scrape_phase()

        if self.predict_file:
            self.predict_process.start(self.ip, self.port)
            self.predict_process.block()
            self.predict_process.scrape_phase()

        self.test_is_complete = True

    def cancel(self):
        self.test_cancelled = True
        self.parse_process.canceled = True
        self.model_process.canceled = True
        self.predict_process.canceled = True

    def terminate(self):
        self.test_is_terminated = True
        try:
          self.parse_process.terminate()
          self.model_process.terminate()
          self.predict_process.terminate()
        except OSError:
          pass

    def get_passed(self):
        return self.parse_process.get_passed() and self.model_process.get_passed() and self.predict_process.get_passed()

    def get_completed(self):
        return self.parse_process.get_completed() and self.model_process.get_completed() and self.predict_process.get_completed()

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

        self.start_epoch_ms = time.time()
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

        print self.test_root_dir
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
        #ip = self.cloud.get_ip()
        #port = self.cloud.get_port()
        test_dir = os.path.join(self.test_root_dir, testDir)
        test_short_dir = testDir
        print "ALLOOHAA"
        #print ip
        #print port
        print test_dir
        print test_short_dir
        print "ALLLOOHAAA"

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

        ip = self.cloud.get_ip()
        port = self.cloud.get_port()
        print "MMMOOOOAOAOAOAOA"
        print ip
        print port
        # Do _one_ test at a time
        while len(self.tests_not_started) > 0:
            test = self.tests_not_started.pop(0)
            test.ip = ip
            test.port = port
            test.do_test()

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

