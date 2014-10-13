#!/usr/bin/python

import json
import sys
import os
import time
import random
import getpass
import re
import atexit
import paramiko
import md5
import errno
import PerfUtils
import requests


class H2OUseCloudNode:
    """  
    A class representing one node in an H2O cloud which was specified by the user.
    Don't try to build or tear down this kind of node.

    use_ip: The given ip of the cloud.
    use_port: The given port of the cloud.
    """

    def __init__(self, use_ip, use_port):
        self.use_ip = use_ip
        self.use_port = use_port

    def start(self):
        pass

    def stop(self):
        pass

    def terminate(self):
        pass

    def get_ip(self):
        return self.use_ip

    def get_port(self):
        return self.use_port


class H2OUseCloud:
    """  
    A class representing an H2O cloud which was specified by the user.
    Don't try to build or tear down this kind of cloud.
    """

    def __init__(self, cloud_num, use_ip, use_port):
        self.cloud_num = cloud_num
        self.use_ip = use_ip
        self.use_port = use_port

        self.nodes = []
        node = H2OUseCloudNode(self.use_ip, self.use_port)
        self.nodes.append(node)

    def start(self):
        pass

    def wait_for_cloud_to_be_up(self):
        pass

    def stop(self):
        pass

    def terminate(self):
        pass

    def get_ip(self):
        node = self.nodes[0]
        return node.get_ip()

    def get_port(self):
        node = self.nodes[0]
        return node.get_port()

    def all_ips(self):
        res = []
        for node in self.nodes:
            res += [node.get_ip()]
        return ','.join(res)

    def all_pids(self):
        res = []
        for node in self.nodes:
            res += [node.request_pid()]
        return ','.join(res)


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

    def __init__(self, cloud_num, nodes_per_cloud, node_num, cloud_name, h2o_jar, ip, base_port,
                 xmx, output_dir, isEC2):
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
        @param isEC2: Whether or not this node is an EC2 node.
        @return: The node object.
        """
        self.cloud_num = cloud_num
        self.nodes_per_cloud = nodes_per_cloud
        self.node_num = node_num
        self.cloud_name = cloud_name
        self.h2o_jar = h2o_jar
        self.ip = ip
        self.base_port = base_port
        self.xmx = xmx
        self.output_dir = output_dir
        self.isEC2 = isEC2

        self.addr = self.ip
        self.http_addr = self.ip
        self.username = getpass.getuser()
        self.password = getpass.getuser()
        if self.isEC2:
            self.username = 'ubuntu'
            self.password = None
        self.ssh = paramiko.SSHClient()
        policy = paramiko.AutoAddPolicy()
        self.ssh.set_missing_host_key_policy(policy)
        self.ssh.load_system_host_keys()

        if self.password is None:
            self.ssh.connect(self.addr, username=self.username)
        else:
            self.ssh.connect(self.addr, username=self.username, password=self.password)

        # keep connection - send keepalive packet evety 5minutes
        self.ssh.get_transport().set_keepalive(300)
        self.uploaded = {}

        self.port = -1

        self.output_file_name = ""
        self.error_file_name = ""
        self.terminated = False

        # Choose my base port number here.  All math is done here.  Every node has the same
        # base_port and calculates it's own my_base_port.
        ports_per_node = 2
        self.my_base_port = \
            int(self.base_port) + \
            int(self.cloud_num * self.nodes_per_cloud * ports_per_node) + \
            int(self.node_num * ports_per_node)

    def open_channel(self):
        ch = self.ssh.get_transport().open_session()
        ch.get_pty()  # force the process to die without the connection
        return ch

    def get_ticks(self):
        """
        Get process_total_ticks, system_total_ticks, sys_idle_ticks.
        """
        #poll on url until get a valid http response
        max_retries = 5
        m = 0
        got_url_sys = False
        got_url_proc = False
        while m < max_retries:
            if m != 0:
                print "DEBUG: Restarting serve_proc!"
                print "Stopping service"
                # cmd_serve = "ps -efww | grep 0xdiag | awk '{print %2}' | xargs kill"
                # tryKill = self.open_channel()
                # tryKill.exec_command(cmd_serve)
                
                print "Starting service"
                # cmd_serve = ["python", "/home/0xdiag/serve_proc.py"]
                # self.channelServe = self.open_channel()
                # self.channelServe.exec_command(' '.join(cmd_serve))
            r_sys = ""
            r_proc = ""
            print "Performing try : " + str(m) + " out of total tries = " + str(max_retries)
            url_sys = "http://{}:{}/stat".format(self.ip, 8000)
            url_proc = "http://{}:{}/{}/stat".format(self.ip, 8000, self.pid)
            try:
              r_sys = requests.get(url_sys, timeout=5).text.split('\n')[0]
              r_proc = requests.get(url_proc, timeout=5).text.strip().split()
            except:
              m += 1
              continue  # usually timeout, but just catch all and continue, error out downstream.
            if r_sys == "" or r_proc == "":
                m += 1
                continue
            if not got_url_sys:
                if not ("404" and "not" and "found") in r_sys:
                    got_url_sys = True

            if not got_url_proc:
                if not ("404" and "not" and "found") in r_proc:
                    got_url_proc = True

            if got_url_proc and got_url_sys:
                break

            m += 1
            time.sleep(2)
            try:
                os.system("ps -efww | grep H2O_perfTest_jenkins | awk '{print $2}' | xargs kill")
                os.system("ssh -l jenkins mr-0xb1 'ps -efww | grep H2O_perfTest_jenkins | awk '{print $2}' | xargs kill'")
                os.system("ssh -l jenkins mr-0xb2 'ps -efww | grep H2O_perfTest_jenkins | awk '{print $2}' | xargs kill'")
                os.system("ssh -l jenkins mr-0xb3 'ps -efww | grep H2O_perfTest_jenkins | awk '{print $2}' | xargs kill'")
            except:
                print "TRIED TO ANY RUNNING PERF JENKINS!"
            time.sleep(1)

        if not (got_url_proc and got_url_sys):
            print "Max retries on /proc scrape exceeded! Did the JVM properly start?"
            return -1
            #raise Exception("Max retries on /proc scrape exceeded! Did the JVM properly start?")

        url_sys = "http://{}:{}/stat".format(self.ip, 8000)
        url_proc = "http://{}:{}/{}/stat".format(self.ip, 8000, self.pid)
        r_sys = requests.get(url_sys, timeout=10).text.split('\n')[0]
        r_proc = requests.get(url_proc, timeout=10).text.strip().split()

        sys_user = int(r_sys.split()[1])
        sys_nice = int(r_sys.split()[2])
        sys_syst = int(r_sys.split()[3])
        sys_idle = int(r_sys.split()[4])
        sys_total_ticks = sys_user + sys_nice + sys_syst + sys_idle

        try:
            print "DEBUGGING /proc scraped values served up: "
            print r_proc
            print " End of try 1."

            proc_utime = int(r_proc[13])
            proc_stime = int(r_proc[14])
            process_total_ticks = proc_utime + proc_stime
        except:
            print "DEBUGGING /proc/<pid>/"
            print "This is try 2... Try 1 failed!"
            print "Did H2O shutdown first before this scrape occured?"
            print r_proc
            print "End of try 2...."
            r_proc = requests.get(url_proc).text.strip().split()
            proc_utime = int(r_proc[13])
            proc_stime = int(r_proc[14])
            process_total_ticks = proc_utime + proc_stime

        return {"process_total_ticks": process_total_ticks, "system_total_ticks": sys_total_ticks,
                "system_idle_ticks": sys_idle}

    def is_contaminated(self):
        """
        Checks for contamination.
        @return: 1 for contamination, 0 for _no_ contamination
        """
        cur_ticks = self.get_ticks()
        first_ticks = self.first_ticks
        if cur_ticks != -1 and first_ticks != -1:
            proc_delta = cur_ticks["process_total_ticks"] - first_ticks["process_total_ticks"]
            sys_delta = cur_ticks["system_total_ticks"] - first_ticks["system_total_ticks"]
            idle_delta = cur_ticks["system_idle_ticks"] - first_ticks["system_idle_ticks"]

            sys_frac = 100 * (1 - idle_delta * 1. / sys_delta)
            proc_frac = 100 * (proc_delta * 1. / sys_delta)

            print "DEBUG: sys_frac, proc_frac"
            print sys_frac, proc_frac
            print ""
            print ""

            #20% diff
            if proc_frac + 5 <= sys_frac:
                self.is_contaminated = True
                return 1
            return 0
        return 0

    def start_remote(self):
        """
        Start one node of H2O.
        (Stash away the self.child and self.pid internally here.)

        @return: none
        """
        #upload flat_file
        #upload aws_creds
        #upload hdfs_config

        cmd = ["java",
               "-Xmx" + self.xmx,
               #"-ea",
               "-jar", self.uploaded[self.h2o_jar],
               "-name", self.cloud_name,
               "-port", str(self.my_base_port)]

        # Add S3N credentials to cmd if they exist.
        ec2_hdfs_config_file_name = os.path.expanduser("/home/spencer/.ec2/core-site.xml")
        if os.path.exists(ec2_hdfs_config_file_name):
            cmd.append("-hdfs_config")
            cmd.append(ec2_hdfs_config_file_name)

        self.output_file_name = "java_" + str(self.cloud_num) + "_" + str(self.node_num)

        self.error_file_name = "java_" + str(self.cloud_num) + "_" + str(self.node_num)

        cmd = ' '.join(cmd)
        self.channel = self.open_channel()
        self.stdouterr = ""  # somehow cat outfile & errorfile?

        outfd, self.output_file_name = PerfUtils.tmp_file(prefix="remoteH2O-" + self.output_file_name, suffix=".out",
                                                          directory=self.output_dir)
        errfd, self.error_file_name = PerfUtils.tmp_file(prefix="remoteH2O-" + self.error_file_name, suffix=".err",
                                                         directory=self.output_dir)

        PerfUtils.drain(self.channel.makefile(), outfd)
        PerfUtils.drain(self.channel.makefile_stderr(), errfd)
        self.channel.exec_command(cmd)

        cmd_serve = ["python", "/home/0xdiag/serve_proc.py"]
        self.channelServe = self.open_channel()
        self.channelServe.exec_command(' '.join(cmd_serve))

        @atexit.register
        def kill_process():
            try:
                try:
                    self.stop_remote()
                    self.channel.exec_command('exit')
                    self.ssh.close()
                except:
                    pass
                try:
                    self.stop_local()
                except:
                    pass
            except OSError:
                pass

        print "+ CMD: " + cmd

    def request_pid(self):
        """
        Use a request for /Cloud.json and look for pid.
        """
        name = self.ip + ":" + self.port
        time.sleep(3)
        r = requests.get("http://" + name + "/Cloud.json")
        name = "/" + name
        j = json.loads(r.text)
        for node in j["nodes"]:
            if node["name"] == name:
                return node["PID"]

    def scrape_port_from_stdout_remote(self):
        """
        Look at the stdout log and figure out which port the JVM chose.
        Write this to self.port.
        This call is blocking.
        Exit if this fails.

        @return: none
        """
        retries = 30
        while retries > 0:
            if self.terminated:
                return
            f = open(self.output_file_name, "r")
            s = f.readline()
            while len(s) > 0:
                if self.terminated:
                    return
                match_groups = re.search(r"Listening for HTTP and REST traffic on  http://(\S+):(\d+)", s)
                if match_groups is not None:
                    port = match_groups.group(2)
                    if port is not None:
                        self.port = port
                        self.pid = self.request_pid()
                        f.close()
                        print("H2O Cloud {} Node {} started with output file {}".format(self.cloud_num,
                                                                                        self.node_num,
                                                                                        self.output_file_name))
                        time.sleep(1)
                        self.first_ticks = self.get_ticks()
                        return

                s = f.readline()

            f.close()
            retries -= 1
            if self.terminated:
                return
            time.sleep(1)

        print("")
        print("ERROR: Too many retries starting cloud.")
        print("")
        sys.exit(1)

    def stop_remote(self):
        """
        Normal node shutdown.
        Ignore failures for now.

        @return: none
        """
        try:
            requests.get("http://" + self.ip + ":" + str(self.port) + "/Shutdown.html", timeout=1)
            try:
                r2 = requests.get("http://" + self.ip + ":" + str(self.port) + "/Cloud.html", timeout=2)
            except Exception, e:
                pass
        except Exception, e:
            pass
        try:
            try:
                self.channel.exec_command('exit')
                self.ssh.close()
            except:
                pass
            try:
                self.stop_local()
            except:
                pass
        except OSError:
            pass
        try:
            requests.get("http://" + self.ip + ":" + str(self.port) + "/Shutdown.html", timeout=1)
        except Exception, e:
            # print "Got Exception trying to shutdown H2O:"
            pass
        print "Successfully shutdown h2o!"
        self.pid = -1

    def stop_local(self):
        """ 
        Normal node shutdown.
        Ignore failures for now.

        @return: none
        """
        #TODO: terminate self.child
        try:
            requests.get(self.ip + ":" + str(self.port) + "/Shutdown.html")
        except Exception, e:
            pass
        self.pid = -1

    def terminate_remote(self):
        """
        Terminate a running node.  (Due to a signal.)

        @return: none
        """
        self.terminated = True
        self.stop_remote()

    def terminate_local(self):
        """ 
        Terminate a running node.  (Due to a signal.)

        @return: none
        """
        self.terminated = True
        self.stop_local()

    def get_ip(self):
        """ Return the ip address this node is really listening on. """
        return self.ip

    def get_output_file_name(self):
        """ Return the directory to the output file name. """
        return self.output_file_name

    def get_error_file_name(self):
        """ Return the directory to the error file name. """
        return self.error_file_name

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

    def __init__(self, cloud_num, hosts_in_cloud, nodes_per_cloud, h2o_jar, base_port, output_dir, isEC2, remote_hosts):
        """  
        Create a cloud.
        See node definition above for argument descriptions.

        @return: The cloud object.
        """
        self.cloud_num = cloud_num
        self.nodes_per_cloud = nodes_per_cloud
        self.h2o_jar = h2o_jar
        self.base_port = base_port
        self.output_dir = output_dir
        self.isEC2 = isEC2
        self.remote_hosts = remote_hosts
        self.hosts_in_cloud = hosts_in_cloud

        # Randomly choose a five digit cloud number.
        n = random.randint(10000, 99999)
        user = getpass.getuser()
        user = ''.join(user.split())

        self.cloud_name = "H2O_perfTest_{}_{}".format(user, n)
        self.nodes = []
        self.jobs_run = 0

        for node_num, node_ in enumerate(self.hosts_in_cloud):
            node = H2OCloudNode(self.cloud_num, self.nodes_per_cloud,
                                node_num, self.cloud_name, self.h2o_jar,
                                node_['ip'],
                                node_['port'],
                                #self.base_port,
                                node_['memory_bytes'],
                                self.output_dir, isEC2)
            self.nodes.append(node)
        self.distribute_h2o()

    def distribute_h2o(self):
        """
        Distribute the H2O to the remote hosts.
        @return: none.
        """
        f = self.h2o_jar

        def prog(sofar, total):
            # output is bad for jenkins.
            username = getpass.getuser()
            if username != 'jenkins':
                p = int(10.0 * sofar / total)
                sys.stdout.write('\rUploading jar [%s%s] %02d%%' % ('#' * p, ' ' * (10 - p), 100 * sofar / total))
                sys.stdout.flush()

        for node in self.nodes:
            m = md5.new()
            m.update(open(f).read())
            m.update(getpass.getuser())
            dest = '/tmp/' + m.hexdigest() + "-" + os.path.basename(f)
            print "Uploading h2o jar to: " + dest + "on " + node.ip
            sftp = node.ssh.open_sftp()
            try:
                sftp.stat(dest)
                print "Skipping upload of file {0}. File {1} exists on remote side!".format(f, dest)
            except IOError, e:
                if e.errno == errno.ENOENT:
                    sftp.put(f, dest, callback=prog)
            finally:
                sftp.close()
            node.uploaded[f] = dest
            # sys.stdout.flush()

    def check_contaminated(self):
        """
        Each node checks itself for contamination.
        @return: True if contaminated, False if _not_ contaminated
        """
        for node in self.nodes:
            if node.is_contaminated():
                return [1, "Node " + node.ip + " was contaminated."]

        return [0, " "]

    def start_remote(self):
        """
        Start an H2O cloud remotely.
        @return: none
        """
        for node in self.nodes:
            node.start_remote()

    def start_local(self):
        """  
        Start H2O cloud.
        The cloud is not up until wait_for_cloud_to_be_up() is called and returns.

        @return: none
        """
        if self.nodes_per_cloud > 1:
            print("")
            print("ERROR: Unimplemented: wait for cloud size > 1.")
            print("")
            sys.exit(1)

        for node in self.nodes:
            node.start_local()

    def wait_for_cloud_to_be_up(self):
        """  
        Blocking call ensuring the cloud is available.

        @return: none
        """
        if self.remote_hosts:
            self._scrape_port_from_stdout_remote()
        else:
            self._scrape_port_from_stdout_local()

    def stop_remote(self):
        """  
        Normal cloud shutdown.

        @return: none
        """
        for node in self.nodes:
            node.stop_remote()

    def stop_local(self):
        """  
        Normal cloud shutdown.

        @return: none
        """
        for node in self.nodes:
            node.stop_local()

    def all_ips(self):
        res = []
        for node in self.nodes:
            res += [node.get_ip()]
        return ','.join(res)

    def all_pids(self):
        res = []
        for node in self.nodes:
            res += [node.request_pid()]
        return ','.join(res)

    def terminate_remote(self):
        """  
        Terminate a running cloud.  (Due to a signal.)

        @return: none
        """
        for node in self.nodes:
            node.terminate_remote()

    def terminate_local(self):
        """  
        Terminate a running cloud.  (Due to a signal.)

        @return: none
        """
        for node in self.nodes:
            node.terminate_local()

    def get_ip(self):
        """ Return an ip to use to talk to this cloud. """
        node = self.nodes[0]
        return node.get_ip()

    def get_port(self):
        """ Return a port to use to talk to this cloud. """
        node = self.nodes[0]
        return node.get_port()

    def _scrape_port_from_stdout(self):
        for node in self.nodes:
            node.scrape_port_from_stdout()

    def _scrape_port_from_stdout_remote(self):
        for node in self.nodes:
            node.scrape_port_from_stdout_remote()

    def __str__(self):
        s = ""
        s += "cloud {}\n".format(self.cloud_num)
        s += "    name:     {}\n".format(self.cloud_name)
        s += "    jobs_run: {}\n".format(self.jobs_run)
        for node in self.nodes:
            s += str(node)
        return s
