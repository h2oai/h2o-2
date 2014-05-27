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
import atexit
import paramiko
import md5
import errno
import PerfUtils
import stat
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

    def __init__(self, cloud_num, nodes_per_cloud, node_num, cloud_name, h2o_jar, ip, base_port, xmx, output_dir, isEC2):
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
            self.base_port + \
            (self.cloud_num * self.nodes_per_cloud * ports_per_node) + \
            (self.node_num * ports_per_node)

    def open_channel(self):
        ch = self.ssh.get_transport().open_session()
        ch.get_pty() # force the process to die without the connection
        return ch

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
               "-baseport", str(self.my_base_port)]

        # Add S3N credentials to cmd if they exist.
        ec2_hdfs_config_file_name = os.path.expanduser("/home/spencer/.ec2/core-site.xml")
        if (os.path.exists(ec2_hdfs_config_file_name)):
            cmd.append("-hdfs_config")
            cmd.append(ec2_hdfs_config_file_name)

        self.output_file_name = "java_" + str(self.cloud_num) + "_" + str(self.node_num)
    
        self.error_file_name = "java_" + str(self.cloud_num) + "_" + str(self.node_num)

        cmd = ' '.join(cmd)
        self.channel = self.open_channel()
        self.stdouterr = "" #somehow cat outfile & errorfile?

        outfd, self.output_file_name = PerfUtils.tmp_file(prefix = "remoteH2O-" + self.output_file_name, suffix = ".out", directory = self.output_dir)
        errfd, self.error_file_name = PerfUtils.tmp_file(prefix = "remoteH2O-" + self.error_file_name, suffix = ".err", directory = self.output_dir)

        PerfUtils.drain(self.channel.makefile(), outfd)
        PerfUtils.drain(self.channel.makefile_stderr(), errfd)
        self.channel.exec_command(cmd)

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

    def scrape_port_from_stdout_remote(self):
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
            requests.get("http://" + self.ip + ":" + self.port + "/Shutdown.html", timeout=5)
            try:
                r2 = requests.get("http://" + self.ip + ":" + self.port + "/Cloud.html", timeout = 2)
            except Exception, e:
                pass
        except Exception, e:
            print "Successfully shutdown h2o!"
            pass
        self.pid = -1

    def stop_local(self):
        """ 
        Normal node shutdown.
        Ignore failures for now.

        @return: none
        """
        #TODO: terminate self.child
        try:
            requests.get(self.ip + ":" + self.port + "/Shutdown.html")
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
                                self.base_port,
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
            if username!='jenkins':
                p = int(10.0 * sofar / total)
                sys.stdout.write('\rUploading jar [%s%s] %02d%%' % ('#'*p, ' '*(10-p), 100*sofar/total))
                sys.stdout.flush()
        for node in self.nodes:
            m = md5.new()
            m.update(open(f).read())
            m.update(getpass.getuser())
            dest = '/tmp/' +m.hexdigest() +"-"+ os.path.basename(f)
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
        if (self.nodes_per_cloud > 1):
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
