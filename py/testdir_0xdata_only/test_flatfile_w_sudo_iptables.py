import unittest, os, time, sys
sys.path.extend(['.','..','../..','py'])
import h2o, h2o_cmd, h2o_import as h2i

def runLinuxCmds(cmds):
    for c in cmds:
        # h2o.verboseprint(c)
        print c
        # FIX! this should execute the command on the other machine?
        # if local machine is part of the cloud, I guess that gives some amount 
        # of asymmetric testing, so good enough? (pain to export execution as root to other machines)
        os.system("sudo " + c)

def showIptables():
    print "\nshowing iptables -L now"
    cmds = ["iptables -L"]
    runLinuxCmds(cmds)

def allAcceptIptables():
    print "Stopping firewall and allowing everyone..."
    cmds = []
    cmds.append("iptables -F")
    cmds.append("iptables -X")
    cmds.append("iptables -t nat -F")
    cmds.append("iptables -t nat -X")
    cmds.append("iptables -t mangle -F")
    cmds.append("iptables -t mangle -X")
    cmds.append("iptables -P INPUT ACCEPT")
    cmds.append("iptables -P FORWARD ACCEPT")
    cmds.append("iptables -P OUTPUT ACCEPT")
    runLinuxCmds(cmds)

# not used right now
def allAcceptIptablesMethod2():
    print "Stopping firewall and allowing everyone..."
    cmds = []
    cmds.append("iptables -flush")
    cmds.append("iptables -delete-chain")
    cmds.append("iptables -table filter -flush")
    cmds.append("iptables -table nat -delete-chain")
    cmds.append("iptables -table filter -delete-chain")
    cmds.append("iptables -table nat -flush")
    runLinuxCmds(cmds)

def multicastAcceptIptables():
    print "Enabling Multicast (only), send and receive"
    cmds = []
    cmds.append("iptables -A INPUT  -m pkttype --pkt-type multicast -j ACCEPT")
    cmds.append("iptables -A INPUT  --protocol igmp -j ACCEPT")
    cmds.append("iptables -A INPUT  --dst '224.0.0.0/4' -j ACCEPT")
    cmds.append("iptables -A OUTPUT -m pkttype --pkt-type multicast -j ACCEPT")
    cmds.append("iptables -A OUTPUT --protocol igmp -j ACCEPT")
    cmds.append("iptables -A OUTPUT --dst '224.0.0.0/4' -j ACCEPT")
    runLinuxCmds(cmds)

def multicastDropReceiveIptables():
    print "Disabling Multicast (only), receive only"
    cmds = []
    cmds.append("iptables -A INPUT  -m pkttype --pkt-type multicast -j DROP")
    cmds.append("iptables -A INPUT  --protocol igmp -j DROP")
    cmds.append("iptables -A INPUT  --dst '224.0.0.0/4' -j DROP")
    runLinuxCmds(cmds)

def multicastBlockSendIptables():
    # I guess this doesn't cause IOexception to java, since the block is invisible to java?
    print "Disabling Multicast (only), send only"
    cmds = []
    cmds.append("iptables -A OUTPUT -m pkttype --pkt-type multicast -j DROP")
    cmds.append("iptables -A OUTPUT --protocol igmp -j DROP")
    cmds.append("iptables -A OUTPUT --dst '224.0.0.0/4' -j DROP")
    runLinuxCmds(cmds)

class Basic(unittest.TestCase):
    def tearDown(self):
        h2o.check_sandbox_for_errors()

    @classmethod
    def setUpClass(cls):

        global nodes_per_host
        # FIX! will this override the json that specifies the multi-host stuff
        nodes_per_host = 4

        print "\nOnly does sudo locally. Something to think about when you put together"
        print "the list of machines in your json. Okay to just break one machine multicast!"

        print "\nThis test may prompt for sudo passwords for executing iptables on linux"
        print "Don't execute as root. or without understanding 'sudo iptables' and this video"
        print "\nhttp://www.youtube.com/watch?v=OWwOJlOI1nU"

        print "\nIt will use pytest_config-<username>.json for multi-host"
        print "Want to run with multicast send or receive disabled somehow in the target cloud"
        print "The test tries to do it with iptables"

        print "\nIf the test fails, you'll have to fix iptables to be how you want them"
        print "Typically 'iptables -F' should be enough."

        print "\nDon't run this on 172.16.2.151 which has sshguard enabled in its iptables"
        print "Only for linux. hopefully centos and ubuntu. don't know about mac"

        print "\nNormally you'll want this to run with -v to see hangs in cloud building"

        print "\nThis is how iptables is at start:"
        showIptables()

    @classmethod
    def tearDownClass(cls):
        h2o.tear_down_cloud()
        allAcceptIptables()
        showIptables()

    def test_A(self):
        print "\nno flatfile, Build allowing anything"
        allAcceptIptables()
        showIptables()
        h2o.init(nodes_per_host, use_flatfile=False)
        h2o.tear_down_cloud()

    def test_B(self):
        print "\nwith flatfile, Build allowing anything"
        allAcceptIptables()
        showIptables()
        h2o.init(nodes_per_host, use_flatfile=True)
        h2o.tear_down_cloud()

    def test_C_no_mc_rcv(self):
        print "\nwith flatfile, with multicast disabled"
        allAcceptIptables()
        multicastDropReceiveIptables()
        showIptables()
        h2o.init(nodes_per_host, use_flatfile=True)
        h2o.tear_down_cloud()

    def test_D_no_mc_snd(self):
        print "\nwith flatfile, with multicast disabled"
        allAcceptIptables()
        multicastBlockSendIptables()
        showIptables()
        h2o.init(nodes_per_host, use_flatfile=True)
        h2o.tear_down_cloud()

    def test_E_no_mc_snd_no_mc_rcv(self):
        print "\nwith flatfile, with multicast disabled"
        allAcceptIptables()
        multicastDropReceiveIptables()
        multicastBlockSendIptables()
        showIptables()
        h2o.init(nodes_per_host, use_flatfile=True)
        h2o.tear_down_cloud()

    def test_F_no_mc_loop(self):
        print "\nwith flatfile, with multicast disabled, and RF, 5 trials"
        allAcceptIptables()
        multicastDropReceiveIptables()
        showIptables()

        for x in range(1,5):
            h2o.init(nodes_per_host, use_flatfile=True)
            parseResult = h2i.import_parse(bucket='smalldata', path='poker/poker1000', schema='put')
            h2o_cmd.runRF(parseResult=parseResult, trees=50, timeoutSecs=10)
            h2o.tear_down_cloud()
            h2o.verboseprint("Waiting", nodes_per_host,
                "seconds to avoid OS sticky port problem")
            time.sleep(nodes_per_host)
            print "Trial", x
            sys.stdout.write('.')
            sys.stdout.flush()

if __name__ == '__main__':
    h2o.unit_main()
