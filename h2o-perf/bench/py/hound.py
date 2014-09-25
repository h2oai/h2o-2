import os
import sys
import paramiko
from h2oPerf import PerfUtils


class ssh_connect:
    def __init__(self):
        self.ssh = paramiko.SSHClient()
        policy = paramiko.AutoAddPolicy()
        self.ssh.set_missing_host_key_policy(policy)
        self.ssh.load_system_host_keys()
        self.ssh.connect("172.16.2.171", username="0xperf", password="0xperf")
        # keep connection - send keepalive packet evety 5minutes
#        self.ssh.get_transport().set_keepalive(300)

    def open_channel(self):
        ch = self.ssh.get_transport().open_session()
        ch.get_pty()  # force the process to die without the connection
        return ch


def main(test_run_id, ips, pids, name):
    split_pids = pids.split(',')
    ssh = ssh_connect()

    cmd = ["python", "/home/0xperf/HOUND/unleash_the_hounds.py", str(test_run_id), pids, ips, name]

#    output_file_name = "hound_" + str('_'.join(split_pids))
#    error_file_name = "hound_" + str('_'.join(split_pids))

    cmd = ' '.join(cmd)

#    channel = open_channel()

#    this_path = os.path.dirname(os.path.realpath(__file__))
#    output_dir = os.path.join(this_path, "results")
#    outfd, output_file_name = PerfUtils.tmp_file(prefix="remoteH2O-" + output_file_name,
#                                                 suffix=".out", directory=output_dir)
#    errfd, error_file_name = PerfUtils.tmp_file(prefix="remoteH2O-" + error_file_name,
#                                                suffix=".err", directory=output_dir)
#
#    PerfUtils.drain(channel.makefile(), outfd)
#    PerfUtils.drain(channel.makefile_stderr(), errfd)
    ssh.ssh.exec_command(cmd)

if __name__ == "__main__":
    test_run_id = sys.argv[1]
    all_pids = sys.argv[2]
    all_ips = sys.argv[3]
    test_name = sys.argv[4]
    main(test_run_id, all_ips, all_pids, test_name)
