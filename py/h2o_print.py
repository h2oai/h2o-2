

# some fun to match michal's use of green in his messaging in ec2_cmd.py
# generalize like http://stackoverflow.com/questions/287871/print-in-terminal-with-colors-using-python
class bcolors:
    PURPLE = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'

    def disable(self):
        self.PURPLE = ''
        self.BLUE = ''
        self.GREEN = ''
        self.YELLOW = ''
        self.RED = ''
        self.ENDC = ''

# make these compatible with multiple args like print?
def green_print(*args):
    # the , a the end means no eol@
    for msg in args:
        print bcolors.GREEN + str(msg) + bcolors.ENDC,
    print

def blue_print(*args):
    for msg in args:
        print bcolors.BLUE + str(msg) + bcolors.ENDC,
    print

def yellow_print(*args):
    for msg in args:
        print bcolors.YELLOW + str(msg) + bcolors.ENDC, 
    print

def red_print(*args):
    for msg in args:
        print bcolors.RED + str(msg) + bcolors.ENDC,
    print

def purple_print(*args):
    for msg in args:
        print bcolors.PURPLE + str(msg) + bcolors.ENDC,
    print

