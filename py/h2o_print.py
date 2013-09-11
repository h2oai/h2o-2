

# some fun to match michal's use of green in his messaging in ec2_cmd.py
# generalize like http://stackoverflow.com/questions/287871/print-in-terminal-with-colors-using-python
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

    def disable(self):
        self.HEADER = ''
        self.OKBLUE = ''
        self.OKGREEN = ''
        self.WARNING = ''
        self.FAIL = ''
        self.ENDC = ''

# make these compatible with multiple args like print?
def ok_green_print(*args):
    # the , a the end means no eol@
    for msg in args:
        print bcolors.OKGREEN + str(msg) + bcolors.ENDC,
    print

def ok_blue_print(*args):
    for msg in args:
        print bcolors.OKBLUE + str(msg) + bcolors.ENDC,
    print

def warning_print(*args):
    for msg in args:
        print bcolors.WARNING + str(msg) + bcolors.ENDC, 
    print

def fail_print(*args):
    for msg in args:
        print bcolors.FAIL + str(msg) + bcolors.ENDC,
    print

def header_print(*args):
    for msg in args:
        print bcolors.HEADER + str(msg) + bcolors.ENDC,
    print

