from H2O import *
from Process import *
from Table import *

import re
import os
import time
import ConfigParser

class Test:
    """
    A Test object is a wrapper of the directory of R files
    "under test." There are at most 3 R files per test:
        1. Parsing
        2. Modeling
        3. Predicting

    Each file represents a phase of the test.
    In addition to these R files, there is a config file.
    """
    def __init__(self, cfg, test_dir, test_short_dir, output_dir, parse_file, model_file, predict_file, perfdb, prefix):
        self.ip = ""
        self.port = -1
        self.aws = False
        self.remote_hosts = False
        self.heap_bytes_per_node = "" #test_run table
        self.total_hosts = 0 #test_run table
        self.total_nodes = 0 #test_run table
        self.instance_type = "none" #test_run table
        self.hosts = [] #test_run_host table

        self.cfg = cfg
        self.__parse_config__()
        self.perfdb = perfdb
        self.test_dir = test_dir
        self.test_short_dir = test_short_dir
        self.output_dir = output_dir
        self.parse_file = parse_file
        self.model_file = model_file
        self.predict_file = predict_file
        self.test_run = ""
        self.test_name = prefix + "_" + os.path.basename(test_dir)

        self.test_is_complete = False
        self.test_cancelled = False
        self.test_terminated = False
        self.start_ms = -1

        self.parse_process = RProc(self.test_dir, self.test_short_dir, self.output_dir, self.parse_file, self.perfdb)
        self.model_process = RProc(self.test_dir, self.test_short_dir, self.output_dir, self.model_file, self.perfdb)
        if predict_file:
            self.predict_process = RProc(self.test_dir, self.test_short_dir, self.output_dir, self.predict_file, self.perfdb)
        else:
            self.predict_process = None

    def __parse_config__(self):
        cfg = ConfigParser.RawConfigParser()
        cfg.read(self.cfg)
        self.aws = cfg.getboolean("H2OBuildInformation", "aws")
        self.remote_hosts = cfg.getboolean("H2OBuildInformation", "remote_hosts")
        self.heap_bytes_per_node = cfg.get("H2OBuildInformation", "heap_bytes_per_node")
        self.total_hosts = cfg.getint("H2OBuildInformation", "total_hosts")
        self.total_nodes = cfg.getint("H2OBuildInformation", "total_nodes")
        self.nodes_per_host = cfg.getint("H2OBuildInformation", "nodes_per_host")
        self.instance_type = cfg.get("H2OBuildInformation", "instance_type")

        #loop over HostN sections if no aws specified
        if not self.aws:
            self.ip = cfg.get("Host1", "ip")
            for host in cfg.sections():
                if host == 'H2OBuildInformation': 
                    continue
                h = {}
                h['host_name'] = host
                h['ip'] = cfg.get(host, "ip")
                h['port'] = cfg.get(host, "port")
                h['num_cpus'] = cfg.get(host, "num_cpus")
                h["memory_bytes"] = cfg.get(host, "memory_bytes")
                h["isEC2"] = self.aws
                self.hosts.append(h)

    def do_test(self, object):
        """
        This call is blocking.

        Perform the test. Each phase is started by passing the ip and port.
        We block until a phase completes.

        Once a phase is complete, the stdouterr is scraped and the database
        tables are updated.
        """
        self.start_ms = int(round(time.time() * 1000))
        self.parse_process.start(self.ip, self.port)
        self.parse_process.block()
        res = self.parse_process.scrape_phase()
        self.test_run.row.update(res)

        self.model_process.start(self.ip, self.port)
        self.model_process.block()
        self.model_process.scrape_phase()
        contamination = PerfUtils.run_contaminated(object)

        if self.predict_process:
            self.predict_process.start(self.ip, self.port)
            self.predict_process.block()
            self.predict_process.scrape_phase()

        self.end_ms = int(round(time.time() * 1000))

        self.test_run.row['timing_passed'] = self.did_time_pass()
        self.test_run.row['correctness_passed'] = self.did_correct_pass()
        self.test_run.row['passed'] = self.did_pass()
        self.test_run.row['contaminated'] = self.contaminated()
        self.test_run.row['contamination_message'] = self.contamination_message()
        self.test_run.row['total_hosts'] = self.total_hosts
        self.test_run.row['total_nodes'] = self.total_nodes
        self.test_run.row['instance_type'] = self.instance_type

        self.test_is_complete = True
        return contamination

    def contamination_message(self):
        message = "Contamination in phase {}. "
        parse_m = message.format("parse") if self.parse_process.contaminated else ""
        model_m = message.format("model") if self.model_process.contaminated else ""
        contam = "No contamination" if (parse_m == "" and model_m == "") else parse_m + model_m
        if self.predict_file:
            predict_m = message.format("predict") if self.predict_process.contaminated else ""
            contam += predict_m
        return MySQLdb.escape_string(contam)
    
    def contaminated(self):
        res = self.parse_process.contaminated and self.model_process.contaminated
        if self.predict_file:
           res = res and self.predict_process.contaminated
        return 1 if res else 0

    def did_time_pass(self):
        parse_pass = self.parse_process.did_time_pass
        model_pass = self.model_process.did_time_pass
        timing_pass = parse_pass and model_pass
        if self.predict_file:
            predict_pass = self.predict_process.did_time_pass
            return 1 if (timing_pass and predict_pass) else 0
        return 1 if timing_pass else 0

    def did_correct_pass(self):
        parse_pass = self.parse_process.did_correct_pass
        model_pass = self.model_process.did_correct_pass
        correct_pass = parse_pass and model_pass
        if self.predict_file:
            predict_pass = self.predict_process.did_correct_pass
            return 1 if (correct_pass and predict_pass) else 0
        return 1 if correct_pass else 0

    def did_pass(self):
        return 1 if (self.did_time_pass() and self.did_correct_pass()) else 0

    def cancel(self):
        self.test_cancelled = True
        self.parse_process.canceled = True
        self.model_process.canceled = True
        if self.predict_file:
            self.predict_process.canceled = True

    def terminate(self):
        self.test_is_terminated = True
        try:
          self.parse_process.terminate()
          self.model_process.terminate()
          if self.predict_file:
              self.predict_process.terminate()
        except OSError:
          pass

    def get_passed(self):
        res = self.parse_process.get_passed() and self.model_process.get_passed()
        if self.predict_file:
            return res and self.predict_process.get_passed()
        return res

    def get_completed(self):
        res = self.parse_process.get_completed() and self.model_process.get_completed()
        if self.predict_file:
            self.predict_process.get_completed()
        return res
