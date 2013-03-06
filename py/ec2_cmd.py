#!/usr/bin/python

import argparse
import boto
import os, time, sys, socket
import h2o_cmd
import json

'''
    Simple EC2 utility:
     
       * to setup clooud of 5 nodes: ./ec2_cmd.py create --instances 5
       * to terminated the cloud   : ./ec2_cmd.py terminate --hosts <host file returned by previous command>
'''

DEFAULT_NUMBER_OF_INSTANCES = 5
DEFAULT_HOSTS_FILENAME = 'ec2-config-{0}.json'

'''
Default EC2 instance setup
'''
DEFAULT_EC2_INSTANCE_CONF = {
  'image_id'        : 'ami-cd9a11a4',
  'security_groups' : [ 'MrJenkinsTest' ],
  'key_name'        : 'mrjenkins_test',
  'instance_type'   : 'm1.xlarge',
  'pem'             : '~/.ec2/keys/mrjenkins_test.pem',
  'username'        : '0xdiag',      
  'aws_credentials' : '~/.ec2/AwsCredentials.properties'
}

''' Memory mappings for instance kinds '''
MEMORY_MAPPING = {
    'm1.large'   : { 'xmx' : 5  },  
    'm1.xlarge'  : { 'xmx' : 11 }, 
    'm2.xlarge'  : { 'xmx' : 13 },  
    'm2.2xlarge' : { 'xmx' : 30 },  
    'm2.4xlarge' : { 'xmx' : 60 },  
    'm3.xlarge'  : { 'xmx' : 11 },  
    'm3.2xlarge' : { 'xmx' : 24 },  
}

''' EC2 API default configuration. The corresponding values are replaces by EC2 user config. '''
EC2_API_RUN_INSTANCE = {
'image_id'        :None,
'min_count'       :1,
'max_count'       :1,
'key_name'        :None,
'security_groups' :None,
'user_data'       :None,
'addressing_type' :None,
'instance_type'   :None,
'placement'       :None,
'monitoring_enabled':False,
'subnet_id'       :None,
'block_device_map':None,
'disable_api_termination':False,
'instance_initiated_shutdown_behavior':None
}

def inheritparams(parent, kid):
    newkid = {}
    for k,v in kid.items():
        if parent.has_key(k):
            newkid[k] = parent[k]

    return newkid

def find_file(base):
    f = base
    if not os.path.exists(f): f = os.path.expanduser(base)
    if not os.path.exists(f): f = os.path.expanduser("~")+ '/' + base
    if not os.path.exists(f):
        raise Exception("unable to find config %s" % base)
    return f


''' Run number of EC2 instance.
Waits forthem and optionaly waits for ssh service.
'''
def run_instances(count, ec2_config, waitForSSH=True):
    '''Create a new reservation for count instances'''
    ec2_config.setdefault('min_count', count)
    ec2_config.setdefault('max_count', count)

    ec2params = inheritparams(ec2_config, EC2_API_RUN_INSTANCE)

    reservation = None
    conn = boto.connect_ec2()
    try:
        reservation = conn.run_instances(**ec2params)
        log('Waiting for {0} EC2 instances {1} to come up, this can take 1-2 minutes.'.format(len(reservation.instances), reservation.instances))
        start = time.time()
        for instance in reservation.instances:
            while instance.update() == 'pending':
               time.sleep(1)
               h2o_cmd.dot()

            if not instance.state == 'running':
                raise Exception('[ec2] Error waiting for running state. Instance is in state {0}.'.format(instance.state))

        log('Instances started in {0} seconds'.format(time.time() - start))
        log('Instances: ')
        for inst in reservation.instances: log("   {0} ({1}) : public ip: {2}, private ip: {3}".format(inst.public_dns_name, inst.id, inst.ip_address, inst.private_ip_address))
        
        if waitForSSH:
            wait_for_ssh([ i.private_ip_address for i in reservation.instances ])

        return reservation
    except:
        print "Unexpected error:", sys.exc_info()
        if reservation:
            terminate_reservation(reservation)
        raise

''' Wait for ssh port 
'''
def ssh_live(ip, port=22):
    return h2o_cmd.port_live(ip,port)

def terminate_reservation(reservation):
    terminate_instances([ i.id for i in reservation.instances ])

def terminate_instances(instances):
    '''terminate all the instances given by its ids'''
    if not instances: return
    conn = boto.connect_ec2()
    log("Terminating instances {0}.".format(instances))
    conn.terminate_instances(instances)
    log("Done")

def stop_instances(instances):
    '''stop all the instances given by its ids'''
    if not instances: return
    conn = boto.connect_ec2()
    log("Stopping instances {0}.".format(instances))
    conn.stop_instances(instances)
    log("Done")

def start_instances(instances):
    '''Start all the instances given by its ids'''
    if not instances: return
    conn = boto.connect_ec2()
    log("Starting instances {0}.".format(instances))
    conn.start_instances(instances)
    log("Done")

def reboot_instances(instances):
    '''Reboot all the instances given by its ids'''
    if not instances: return
    conn = boto.connect_ec2()
    log("Rebooting instances {0}.".format(instances))
    conn.reboot_instances(instances)
    log("Done")

def wait_for_ssh(ips, port=22, skipAlive=True, requiredsuccess=3):
    ''' Wait for ssh service to appear on given hosts'''
    log('Waiting for SSH on following hosts: {0}'.format(ips))
    for ip in ips:
        if not skipAlive or not ssh_live(ip, port): 
            log('Waiting for SSH on instance {0}...'.format(ip))
            count = 0
            while count < requiredsuccess:
                if ssh_live(ip, port):
                    count += 1
                else:
                    count = 0
                time.sleep(1)
                h2o_cmd.dot()


def dump_hosts_config(ec2_config, reservation, filename=DEFAULT_HOSTS_FILENAME):
    if not filename: filename=DEFAULT_HOSTS_FILENAME

    cfg = {}
    cfg['aws_credentials'] = find_file(ec2_config['aws_credentials'])
    cfg['username']        = ec2_config['username'] 
    cfg['key_filename']    = find_file(ec2_config['pem'])
    cfg['use_flatfile']    = True
    cfg['h2o_per_host']    = 1
    cfg['java_heap_GB']    = MEMORY_MAPPING[ec2_config['instance_type']]['xmx']
    cfg['java_extra_args'] = '-XX:MaxDirectMemorySize=1g'
    cfg['base_port']       = 54321
    cfg['ip'] = [ i.private_ip_address for i in reservation.instances ]
    cfg['instances'] = [ { 'id': i.id, 'private_ip_address': i.private_ip_address, 'public_ip_address': i.ip_address, 'public_dns_name': i.public_dns_name } for i in reservation.instances ]
    cfg['reservation_id']  = reservation.id
    # save config
    filename = filename.format(reservation.id)
    with open(filename, 'w+') as f:
        f.write(json.dumps(cfg, indent=4))

    log("Host config dumped into {0}".format(filename))
    log("To terminate instances call:")
    log("python ec2_cmd.py terminate --hosts {0}".format(filename))

def dump_ssh_commands(ec2_config, reservation):
    for i in reservation.instances:
        print "ssh -i ~/.ec2/keys/mrjenkins_test.pem ubuntu@{0}".format(i.private_ip_address) 


def load_ec2_config(config_file):
    if config_file:
        f = find_file(config_file)
        with open(f, 'rb') as fp:
             ec2_cfg = json.load(fp)
    else:
        ec2_cfg = {}

    for k,v in DEFAULT_EC2_INSTANCE_CONF.items():
        ec2_cfg.setdefault(k, v)

    return ec2_cfg

def load_hosts_config(config_file):
    f = find_file(config_file)
    with open(f, 'rb') as fp:
         host_cfg = json.load(fp)
    return host_cfg

def log(msg):
    print "[ec2] ", msg

def invoke_hosts_action(action, hosts_config):
    ids = [ inst['id'] for inst in hosts_config['instances'] ]
    ips = [ inst['private_ip_address'] for inst in hosts_config['instances'] ]

    if (action == 'terminate'):
        terminate_instances(ids)
    elif (action == 'stop'):
        stop_instances(ids)
    elif (action == 'reboot'):
        reboot_instances(ids)
        wait_for_ssh(ips, skipAlive=False, requiredsuccess=10)
    elif (action == 'start'):
        start_instances(ids)
        # FIXME after start instances receive new IPs: wait_for_ssh(ips)
    elif (action == 'distribute_h2o'):
        pass
    elif (action == 'start_h2o'):
        pass
    elif (action == 'stop_h2o'):
        pass

def main():
    parser = argparse.ArgumentParser(description='H2O EC2 instances launcher')
    parser.add_argument('action', choices=['create', 'terminate', 'stop', 'reboot', 'start', 'distribute_h2o', 'start_h2o', 'stop_h2o', 'show_defaults'],  help='EC2 instances action')
    parser.add_argument('--config', help='Configuration file to configure NEW EC2 instances (if not specified default is used - see --show_defaults)', type=str, default=None)
    parser.add_argument('--instances', help='Number of instances to launch', type=int, default=DEFAULT_NUMBER_OF_INSTANCES)
    parser.add_argument('--hosts', help='Hosts file describing existing "EXISTING" EC2 instances ', type=str, default=None)
    args = parser.parse_args()

    if (args.action == 'create'):
        ec2_config = load_ec2_config(args.config)
        log("Config   : {0}".format(ec2_config))
        log("Instances: {0}".format(args.instances))
        reservation = run_instances(args.instances, ec2_config)
        dump_hosts_config(ec2_config, reservation, args.hosts)
        dump_ssh_commands(ec2_config, reservation)
    elif (args.action == 'show_defaults'):
        print "Config    : {0}".format(DEFAULT_EC2_INSTANCE_CONF)
        print "Instances : {0}".format(DEFAULT_NUMBER_OF_INSTANCES)
    else: 
        hosts_config = load_hosts_config(args.hosts)
        invoke_hosts_action(args.action, hosts_config)
        if (args.action == 'terminate'):
            log("Deleting {0} host file.".format(args.hosts))
            os.remove(args.hosts)

if __name__ == '__main__':
    main()


