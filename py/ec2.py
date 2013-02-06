import h2o
import boto
import time, sys, socket

IMAGE_ID = 'ami-d48c2abd'
SECURITY_GROUPS = [ 'h2ocloud' ]
KEY_NAME = '0xdata_Big'
INSTANCE_TYPE = 'm2.4xlarge'
PEM = h2o.find_file('0xdata_Big.pem')

class Ec2Host(h2o.RemoteHost):
    '''
    A RemoteHost backed by Amazon EC2, additionally manages
    - key files
    - upload within the cloud
    - ssh key permissions
    - ssh host checking
    '''
    def __init__(self, instance, key_filename, **kwargs):
        super(Ec2Host, self).__init__(instance.public_dns_name, 'ubuntu', key_filename=key_filename, **kwargs)
        self.instance = instance

    def push_file_to_remotes(self, f, hosts):
        pem = self.upload_file(PEM)
        self.run_cmd('chmod 400 %s' % (pem))
        dest = self.uploaded[f]
        for h in hosts:
            if h == self: continue
            self.run_cmd("scp -o '\''StrictHostKeyChecking no'\'' -o '\''CheckHostIP no'\'' -o '\''HashKnownHosts no'\'' -o '\''UserKnownHostsFile /dev/null'\'' -i %s %s %s@%s:%s" % (pem, dest, h.username, h.addr, dest))
            h.record_file(f, dest)

def run_instances(count=1, **kwargs):
    '''Create a new reservation for count instances'''
    kwargs.setdefault('image_id', IMAGE_ID)
    kwargs.setdefault('security_groups', SECURITY_GROUPS)
    kwargs.setdefault('key_name', KEY_NAME)
    kwargs.setdefault('instance_type', INSTANCE_TYPE)
    kwargs.setdefault('min_count', count)
    kwargs.setdefault('max_count', count)
    reservation = None
    conn = boto.connect_ec2()
    try:
        reservation = conn.run_instances(**kwargs)
        print 'Waiting for ec2 instances to come up, this can take 1-2 minutes.'
        start = time.time()
        for instance in reservation.instances:
            while instance.update() == 'pending':
                sys.stdout.write('.')
                sys.stdout.flush()
                time.sleep(1)

            if not instance.state == 'running':
                raise Exception('Error waiting for running state')
        print ' started in %d seconds' % (time.time() - start)
        return reservation
    except:
        try:
            terminate_instances(reservation)
        except:
            pass
        raise

def terminate_instances(instances):
    '''terminate all the instances in a list or Reservation'''
    if not reservation: return
    if type(instances) is boto.ec2.instance.Reservation:
        instances = [ i for i in instances.instances ]
    conn = boto.connect_ec2()
    conn.terminate_instances([ i.id for i in instances])

def create_hosts(instances, key_filename=PEM):
    '''Create RemoteHosts from a list of instances or a reservation'''
    if type(instances) is boto.ec2.instance.Reservation:
        instances = [ i for i in instances.instances ]
    try:
        return [ Ec2Host(i, key_filename=key_filename) for i in instances ]
    except socket.error, e:
        # Connection refusal is normal. 
        # It just means the node has not started up yet.
        conn_err = e.errno
        if (    conn_err == 61 or   # mac/linux
                conn_err == 54 or
                conn_err == 111 or  # mac/linux
                conn_err == 104 or  # ubuntu (kbn)
                conn_err == 10061): # windows
            time.sleep(10)
            return [ Ec2Host(i, key_filename=key_filename) for i in instances ]
        raise

def build_cloud(hosts, node_count=2, base_port=54321, **kwargs):
    kwargs['use_flatfile'] = h2o.flatfile_name()
    h2o.write_flatfile(node_count=node_count, base_port=base_port, hosts=hosts)
    h2o.upload_jar_to_remote_hosts(hosts, slow_connection=True)
    kwargs.setdefault('node_count', node_count)
    kwargs.setdefault('base_port', base_port)
    kwargs.setdefault('hosts', hosts)
    return h2o.build_cloud(**kwargs)

