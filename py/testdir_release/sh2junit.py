#!/usr/bin/python
import sys, psutil, os, tempfile, argparse, time
sys.path.extend(['.','..','../..','py'])
import h2o_sandbox

# stripped down by similar to what h2o.py has for these functions
# It's possible to do this in bash, but the code becomes cryptic. o
# You can execute this as sh2junit.py <bash command string>

# sh2junit runs the cmd_string as a subprocess, with stdout/stderr going to files in sandbox
# When it completes, check the sandbox for errors (using h2o_sandbox.py
# prints interesting things to stdout. Creates the result xml in the current dire
# with name "sh2junit_<name>.xml"

print "Assumes ./sandbox already exists in current dir. Created by cloud building?"

def sandbox_tmp_file(prefix='', suffix=''):
    return tempfile.mkstemp(prefix=prefix, suffix=suffix, dir='./sandbox')

# belt and suspenders. Do we really need to worry about this?
def terminate_process_tree(pid, including_parent=True):
    parent = psutil.Process(pid)
    for child in parent.get_children(recursive=True):
        child.terminate()
    if including_parent:
        parent.terminate()

def terminate_child_processes():
    me = os.getpid()
    terminate_process_tree(me, including_parent=False)


#**************************************************************************
# Example junit xml
#<?xml version="1.0" encoding="UTF-8"?>
#<testsuites disabled="" errors="" failures="" name="" tests="" time="">
#    <testsuite disabled="" errors="" failures="" hostname="" id="" name="" package="" skipped="" tests="" time="" timestamp="">
#        <properties>
#            <property name="" value=""/>
#        </properties>
#        <testcase assertions="" classname="" name="" status="" time="">
#            <skipped/>
#            <error message="" type=""/>
#            <failure message="" type=""/>
#            <system-out/>
#            <system-err/>
#        </testcase>
#        <system-out/>
#        <system-err/>
#    </testsuite>
#</testsuites>
def create_junit_xml(name, out, err, sandboxErrorMessage, errors=0, elapsed=0):
    # http://junitpdfreport.sourceforge.net/managedcontent/PdfTranslation

    content  = '<?xml version="1.0" encoding="UTF-8" ?>\n'
    content += '    <testsuite failures="0" name="%s" tests="1" errors="%s" time="%0.4f">\n' % (name, errors, elapsed)
    content += '        <testcase name="%s" time="%0.4f">\n' % (name, elapsed)
    content += '            <failure type="ScriptError" message="Script Error"></failure>\n'

    content += '            <system-out>\n'
    content += '<![CDATA[\n'
    content += 'spawn stdout**********************************************************\n'
    content += out
    content += ']]>\n'
    content += '            </system-out>\n'

    content += '            <system-err>\n'
    content += '<![CDATA[\n'
    content += 'spawn stderr**********************************************************\n'
    content += err
    if sandboxErrorMessage:
        content += 'spawn errors from sandbox log parsing*********************************\n'
        # maybe could split this into a 2nd stdout or stder ..see above 
        content += sandboxErrorMessage
    content += ']]>\n'
    content += '            </system-err>\n'

    content += '        </testcase>\n'
    content += '    </testsuite>\n'

    f = open('./sh2junit_' + name + '.xml', 'w')
    f.write(content)
    f.close()

#**************************************************************************
def sh2junit(name='NoName', cmd_string='/bin/ls', timeout=300, **kwargs):
    # split by arbitrary strings of whitespace characters (space, tab, newline, return, formfeed)
    print "cmd_string:", cmd_string
    cmdList = cmd_string.split()
    outfd, outpath = sandbox_tmp_file(prefix=name + '.stdout.', suffix='.log')
    errfd, errpath = sandbox_tmp_file(prefix=name + '.stderr.', suffix='.log')
    print "outpath:", outpath
    print "errpath:", errpath

    start = time.time()
    print "psutil.Popen:", cmdList, outpath, errpath
    ps = psutil.Popen(cmdList, stdin=None, stdout=outfd, stderr=errfd, **kwargs)
    comment = 'PID %d, stdout %s, stderr %s' % (
        ps.pid, os.path.basename(outpath), os.path.basename(errpath))
    print "spawn_cmd", cmd_string, comment
    # timeout=None waits forever. timeout=0 returns immediately.
    # default above is 5 minutes
    # Wait for process termination. Since child:  return the exit code. 
    # If the process is already terminated does not raise NoSuchProcess exception but just return None immediately. 
    # If timeout is specified and process is still alive raises psutil.TimeoutExpired() exception. 
    rc = ps.wait(timeout)
    elapsed = time.time() - start

    out = file(outpath).read()
    err = file(errpath).read()

    # FIX! should we check whether the # of lines in err is 0, or assume the rc tells us?
    if rc:
        print "rc:", rc
        errors = rc
    else: 
        # None means it already completed? 
        # FIX! Also none if we get a timeout exception on this python ..how is that captured?
        errors = 0

    # this prunes to interesting lines
    # error lines are returned. warning/info are printed to our (python stdout)
    # so that's always printed/saved?
    # None if no error
    sandboxErrorMessage = h2o_sandbox.check_sandbox_for_errors(
        LOG_DIR='./sandbox', 
        python_test_name=name, 
        cloudShutdownIsError=True, 
        sandboxIgnoreErrors=True) # don't take exception on error

    if sandboxErrorMessage:
        errors += 1

    create_junit_xml(name, out, err, sandboxErrorMessage, errors=rc, elapsed=elapsed)

    # could have already terminated?
    if rc is None:
        terminate_process_tree(ps.pid, including_parent=True)
        # ps.terminate()
        raise Exception("%s %s timed out after %d\nstdout:\n%s\n\nstderr:\n%s" %
                (name, cmd_string, timeout or 0, out, err))
    elif rc != 0:
        raise Exception("%s %s failed. errors: %s\nstdout:\n%s\n\nstderr:\n%s" % 
                (name, cmd_string, errors, out, err))
    elif errors != 0:
        raise Exception("%s %s has errors %s in ./sandbox log files?.\nstdout:\n%s\n\nstderr:\n%s" % 
                (name, cmd_string, errors, out, err))
    else:
        return (errors, outpath, errpath)


#**************************************************************************
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-name', type=str, help='used to help name the xml/stdout/stderr logs created')
    parser.add_argument('-timeout', type=int, help='secs timeout for the shell subprocess. fail if timeout')
    parser.add_argument('-cmd', '--cmd_string', type=str, help='cmd string to pass to shell subprocess')
    parser.add_argument('rargs', nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if args.name:
        name = args.name
    else:
        name = "NoName"

    if args.cmd_string:
        cmd_string = args.cmd_string
    else:
        # easiest way to handle multiple tokens for command
        # end with -- and this grabs the rest
        # drop the leading '--' if we stopped parsing the rest that way
        if args.rargs:
            print "args.rargs:", args.rargs
            if args.rargs[0]=='--':
                args.rargs[0] = ''
            cmd_string = ' '.join(args.rargs)
        else:
            # placeholder for test
            cmd_string = '/bin/ls'
        
    sh2junit(name=name, cmd_string=cmd_string, timeout=args.timeout)

