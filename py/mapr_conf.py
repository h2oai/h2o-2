#! /usr/bin/python

import boto
import time, sys
import pprint 
import paramiko
import argparse
import ec2_cmd as ec2

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
EC2_PKEY_FILE     = '/home/michal/.ec2/keys/0xdata_Big.pem'

#
# Our small MapR cloud configuration based on BigInstances2{1-4}:
#  - one CLDB node
#  - three ZooKeeper nodes
#  - each node provides MapR filesystem
#  - each node has an mapr user which can perform sudo
#
# Reminder (from mapr tutorials): Once cluster is started:
# - /opt/mapr/bin/maprcli acl edit -type cluster -user mapr:fc
# - sudo passwd mapr (password required only for Web admin)
#

DEFAULT_MAPR_CLUSTER_CONF = {
        'need_sudo'    : True,
        'username'     : 'mapr',
        'name'         : 'mapr_h2o',
        'mapr_home'    : '/opt/mapr',
        'mapr_disks'   : '/home/mapr/disks.txt', 
        'layout' : {
            # service minimal layout
            'zookeepers' : 3, # 3 or 5, odd number, minimal 2
            'cldbs'      : 2,
            'jobtrackers': 2,
            'hbases'     : 2,
            'nfss'       : 2, # more is better
            },
        }
# = END of cluster configuration = 

def main():
    parser = argparse.ArgumentParser(description='MapR EC2 cluster management utility')
    parser.add_argument('-v', '--verbose',     help="verbose", action='store_true')
    parser.add_argument('-r', '--reservation', help="reservation id of created EC2 instances", type=str, required=True)
    parser.add_argument('action', choices=['start', 'stop', 'restart', 'configure', 'list'],  help='MapR cluster action')
    args = parser.parse_args()

    # MapR cluster
    mcluster = MaprCluster.create(DEFAULT_MAPR_CLUSTER_CONF, args.reservation)
    mcluster.connect_all()

    if (args.action == 'start'):
        mcluster.start_all()
    elif (args.action == 'stop'):
        mcluster.stop_all()
    elif (args.action == 'restart'):
        mcluster.stop_all()
        mcluster.start_all()
    elif (args.action == 'configure'):
        mcluster.configure_all()
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

    def mapr_conf_cmd(self, cldbs, zookeepers, mapr_home, sudo_required=False):
        sudo_cmd = ''
        if sudo_required: sudo_cmd = 'sudo '
        return '%s%s/server/configure.sh -C %s -Z %s -N %s' % (sudo_cmd, mapr_home, ','.join(cldbs), ','.join(zookeepers), self.name)

    def mapr_disk_cmd(self, mapr_home, mapr_disks, sudo_required=False):
        sudo_cmd = ''
        if sudo_required: sudo_cmd = 'sudo '
        return '%s%s/server/disksetup -F %s' % (sudo_cmd, mapr_home, mapr_disks)

    def connect_all(self):
        for node in self.nodes:
            node.connect()

    def disconnect_all(self):
        for node in self.nodes:
            node.disconnect()

    def start_all(self):
        # Recommended initialization sequence
        # Start zookeeper on selected nodes
        for node in self.nodes:
            node.start_zookeeper()
        # Start warden on 
        for node in self.nodes:
            node.start_warden()

    def stop_all(self):
        for node in self.nodes:
            node.stop_services()
        
    def configure_all(self):
        for node in self.nodes:
            cmd = self.mapr_conf_cmd(self.cldbs, self.zookeepers, sudo_required=node.need_sudo, mapr_home=node.mapr_home )
            node.configure_mapr(cmd)
        
        for node in self.nodes:
            cmd = self.mapr_disk_cmd(mapr_home=node.mapr_home,mapr_disks=node.mapr_disks,sudo_required=node.need_sudo)
            node.configure_mapr(cmd)

    def __init__(self, name, nodes_conf):
        self.name       = name
        self.nodes_conf = nodes_conf
        
        # Collects IP addresses of cldb nodes and zookeepers
        self.cldbs      = [inst['private_ip'] for inst in filter(lambda x: x['cldb'] == True, self.nodes_conf)]
        self.zookeepers = [inst['private_ip'] for inst in filter(lambda x: x['zookeeper'] == True, self.nodes_conf)]

        self.nodes      = [MaprNode(conf) for conf in self.nodes_conf]


    @classmethod
    def create(clazz, mapr_conf, reservation_id): 
        reservation = ec2.load_ec2_reservation(reservation_id, ec2.DEFAULT_REGION)
        nodes_conf  = []
        remaining   = len(reservation.instances)
        if remaining < 3: zookeepers = 1
        else: zookeepers = mapr_conf['layout']['zookeepers']
        cldbs       = mapr_conf['layout']['cldbs']
        jobtrackers = mapr_conf['layout']['jobtrackers']
        for instance in reservation.instances:
            node_conf = { }
            node_conf['private_ip'] = instance.private_ip_address
            node_conf['public_ip' ] = instance.ip_address
            node_conf['need_sudo' ] = mapr_conf['need_sudo' ]
            node_conf['mapr_home' ] = mapr_conf['mapr_home' ]
            node_conf['mapr_disks'] = mapr_conf['mapr_disks']
            node_conf['username'  ] = mapr_conf['username']
            node_conf['zookeeper' ] = False
            node_conf['cldb'      ] = False
            node_conf['jobtracker'] = False
            if zookeepers > 0:
                node_conf['zookeeper'] = True
                zookeepers -= 1
            if cldbs > 0 and cldbs >= remaining:
                node_conf['cldb'] = True
                cldbs -= 1

            remaining -= 1
            nodes_conf.append(node_conf)

        return MaprCluster(mapr_conf['name'], nodes_conf)
        
def log(s):
    print(s)

def dump_output(prefix, out):
    data = out.read().splitlines()
    for line in data:
        print '%s %s' % (prefix, line)

if __name__ == "__main__":
    main()

