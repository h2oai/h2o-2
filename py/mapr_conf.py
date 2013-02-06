import boto
import time, sys
import pprint 
import paramiko
import h2o
import argparse

############## Before you run the script ##########################
# The script needs to have prepared the following shell properties:
###################################################################
#
#export JAVA_HOME="$(/usr/libexec/java_home)"
#export EC2_PRIVATE_KEY="$(/bin/ls "$HOME"/.ec2/pk-*.pem | /usr/bin/head -1)"
#export EC2_CERT="$(/bin/ls "$HOME"/.ec2/cert-*.pem | /usr/bin/head -1)"
#export EC2_HOME="/usr/local/Library/LinkedKegs/ec2-api-tools/jars"
#export AWS_ACCESS_KEY_ID=`head ~/.ec2/access-key.id`
#export AWS_SECRET_ACCESS_KEY=`head ~/.ec2/access-key.pk`
#

#
# EC2 connect configuration
#
SECURITY_GROUPS = [ 'h2ocloud' ] 
KEY_NAME        = '0xdata_Big' 
INSTANCE_TYPE   = 'm2.4xlarge' 
DEFAULT_MAPR_HOME = '/opt/mapr'
MAPR_CLUSTER_NAME = 'mapr_0xdata'

EC2_PKEY_FILE     = h2o.find_file('0xdata_Big.pem')

#
# Our small MapR cloud configuration based on BigInstances2{1-4}:
#  - one CLDB node
#  - three ZooKeeper nodes
#  - each node provides MapR filesystem
#  - each node has an ubuntu user which can perform sudo
#
INSTANCES_IDs = [
        { 'name': 'BigInstance21', 'id': 'i-3b3da947', 'cldb': True,  'zookeeper': True,  'public_ip': None, 'private_ip': None, 'username': 'ubuntu', 'need_sudo': True, 'mapr_home': DEFAULT_MAPR_HOME,},
        { 'name': 'BigInstance22', 'id': 'i-353da949', 'cldb': False, 'zookeeper': True,  'public_ip': None, 'private_ip': None, 'username': 'ubuntu', 'need_sudo': True, 'mapr_home': DEFAULT_MAPR_HOME,}, 
        { 'name': 'BigInstance23', 'id': 'i-373da94b', 'cldb': False, 'zookeeper': True,  'public_ip': None, 'private_ip': None, 'username': 'ubuntu', 'need_sudo': True, 'mapr_home': DEFAULT_MAPR_HOME,},
        { 'name': 'BigInstance24', 'id': 'i-313da94d', 'cldb': False, 'zookeeper': False, 'public_ip': None, 'private_ip': None, 'username': 'ubuntu', 'need_sudo': True, 'mapr_home': DEFAULT_MAPR_HOME,},
        ]
# = END of cluster configuration = 

def main():
    parser = argparse.ArgumentParser(description='MapR EC2 cluster management utility')
    parser.add_argument('-v', '--verbose', help="verbose", action='store_true')
    parser.add_argument('action', choices=['start', 'stop', 'restart', 'configure', 'list'],  help='MapR cluster action')
    args = parser.parse_args()

    # MapR cluster
    mcluster = MaprCluster(INSTANCES_IDs)
    mcluster.resolve_conf()
    mcluster.connect_all()

    if (args.action == 'start'):
        mcluster.start_all()
    elif (args.action == 'stop'):
        mcluster.stop_all()
    elif (args.action == 'restart'):
        mcluster.stop_all()
        mcluster.start_all()
    elif (args.action == 'configure'):
        mcluster.stop_all()
        mcluster.configure_all()
        mcluster.start_all()
    elif (args.action == 'list'):
        log('Not yet implemented!')

    mcluster.disconnect_all()

class MaprNode(object):

    def __connected(self):
        return self.ssh is not None
    
    def __exec_cmd(self, cmd, dump_stdout=True, dump_stderr=True):
        log('[ssh] executing command \'%s\'' % cmd)
        stdin,stdout,stderr = self.ssh.exec_command(cmd)
        stdin.flush();
        if dump_stdout: dump_output('[ssh] stdout->', stdout)
        if dump_stderr: dump_output('[ssh] stderr->', stderr)

    def __service(self, service, action):
        if self.need_sudo: sudo_cmd = 'sudo '
        cmd = '%sservice %s %s' % (sudo_cmd, service, action)
        self.__exec_cmd(cmd)

    def connect(self):
        if self.__connected(): 
            self.disconnect()
        log('[ssh] Connecting to %s ...' % self.public_ip) 
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(self.public_ip, username=self.username, pkey=paramiko.RSAKey.from_private_key_file(EC2_PKEY_FILE))

    def disconnect(self):
        if self.__connected():
            log('[ssh] Disconnecting from %s ...' % self.public_ip) 
            self.ssh.close()
            self.ssh = None
    
    def configure_mapr(self, cmd):
        if self.__connected():
            self.__exec_cmd(cmd)

    def stop_services(self):
        if self.__connected():
            self.stop_zookeeper()
            self.stop_warden()

    def start_services(self):
        if self.__connected():
            self.start_zookeeper()
            self.start_warden()

    def start_zookeeper(self): 
        if self.__connected():
            if self.zookeeper:
                self.__service('mapr-zookeeper', 'start')

    def stop_zookeeper(self): 
        if self.__connected():
            if self.zookeeper:
                self.__service('mapr-zookeeper', 'stop')

    def start_warden(self):
        if self.__connected():
            self.__service('mapr-warden', 'start')

    def stop_warden(self):
        if self.__connected():
            self.__service('mapr-warden', 'stop')

    def __init__(self, node_conf):
        for key in node_conf:
            setattr(self, key, node_conf[key])

        self.ssh = None

class MaprCluster(object):

    def mapr_conf_cmd(self, cldbs, zookeepers, sudo_required=False,mapr_home=DEFAULT_MAPR_HOME):
        sudo_cmd = ''
        if sudo_required: sudo_cmd = 'sudo '
        return '%s%s/server/configure.sh -C %s -Z %s -N %s' % (sudo_cmd, mapr_home, ','.join(cldbs), ','.join(zookeepers), MAPR_CLUSTER_NAME)

    def connect_all(self):
        for node in self.nodes:
            node.connect()

    def disconnect_all(self):
        for node in self.nodes:
            node.disconnect()

    def start_all(self):
        for node in self.nodes:
            node.start_services()

    def stop_all(self):
        for node in self.nodes:
            node.stop_services()
        
    def configure_all(self):
        for node in self.nodes:
            cmd = self.mapr_conf_cmd(self.cldbs, self.zookeepers, sudo_required=node.need_sudo, mapr_home=node.mapr_home )
            node.configure_mapr(cmd)

    def resolve_conf(self):
        conn = boto.connect_ec2()
        reservations = conn.get_all_instances([iid['id'] for iid in INSTANCES_IDs])
        instances = [i for r in reservations for i in r.instances]

        allRunning = True;
        for (node_conf, inst) in zip(self.nodes_conf, instances):
            #pprint(inst.__dict__)
            if not inst.state == 'running':
                allRunning = False
                log('Instance %s is not running. Please launch it!' % inst.id)
            node_conf['public_ip']  =  inst.ip_address
            node_conf['private_ip'] =  inst.private_ip_address


        # TODO launch the instances
        if not allRunning:
            log('Some of instances do not run! Exiting ...')
            sys.exit(-1)
        
        # Collects IP addresses of cldb nodes and zookeepers
        self.cldbs      = [inst['private_ip'] for inst in filter(lambda x: x['cldb'] == True, self.nodes_conf)]
        self.zookeepers = [inst['private_ip'] for inst in filter(lambda x: x['zookeeper'] == True, self.nodes_conf)]

        self.nodes      = [MaprNode(conf) for conf in self.nodes_conf]

    def __init__(self, nodes_conf):
        self.nodes_conf = nodes_conf
        
def log(s):
    print(s)


def dump_output(prefix, out):
    data = out.read().splitlines()
    for line in data:
        print '%s %s' % (prefix, line)

if __name__ == "__main__":
    main()


