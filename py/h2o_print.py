
import getpass

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

b = bcolors()
# make these compatible with multiple args like print?
def green_print(*args):
    # the , at the end means no eol
    if getpass.getuser()=='jenkins':
        bcolors.disable(b)
    for msg in args:
        print b.GREEN + str(msg) + b.ENDC,
    print

def blue_print(*args):
    if getpass.getuser()=='jenkins':
        bcolors.disable(b)
    for msg in args:
        print b.BLUE + str(msg) + b.ENDC,
    print

def yellow_print(*args):
    if getpass.getuser()=='jenkins':
        bcolors.disable(b)
    for msg in args:
        print b.YELLOW + str(msg) + b.ENDC, 
    print

def red_print(*args):
    if getpass.getuser()=='jenkins':
        bcolors.disable(b)
    for msg in args:
        print b.RED + str(msg) + b.ENDC,
    print

def purple_print(*args):
    if getpass.getuser()=='jenkins':
        bcolors.disable(b)
    for msg in args:
        print b.PURPLE + str(msg) + b.ENDC,
    print

