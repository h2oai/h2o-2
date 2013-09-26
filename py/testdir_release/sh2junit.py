#!/usr/bin/python
import sys, psutil, os, stat, tempfile, argparse, time
sys.path.extend(['.','..','../..','py'])
import h2o_sandbox

# Stripped down, similar to h2o.py has for these functions
# Possible to do this in bash, but the code becomes cryptic.
# You can execute this as sh2junit.py <bash command string>

# sh2junit runs the cmd_string as a subprocess, with stdout/stderr going to files in sandbox
# and stdout to python stdout too.
# When it completes, check the sandbox for errors (using h2o_sandbox.py
# prints interesting things to stdout. Creates the result xml in the current dire
# with name "sh2junit_<name>.xml"

print "Assumes ./sandbox already exists in current dir. Created by cloud building?"
def sandbox_tmp_file(prefix='', suffix=''):
    fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir='./sandbox')
    # make sure the file now exists
    # os.open(path, 'a').close()
    # give everyone permission to read it (jenkins running as 
    # 0xcustomer needs to archive as jenkins
    permissions = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH
    os.chmod(path, permissions)
    return (fd, path)

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
    if errors != 0 and not sandboxErrorMessage:
        content += '            <failure type="Non-zero R exit code" message="Non-zero R exit code"></failure>\n'
    # may or may not be 2 errors (R exit code plus log error
    if errors != 0 and sandboxErrorMessage:
        content += '            <failure type="Error in h2o logs" message="Error in h2o logs"></failure>\n'
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
# belt and suspenders. Do we really need to worry about this?
def terminate_process_tree(pid, including_parent=True):
    parent = psutil.Process(pid)
    for child in parent.get_children(recursive=True):
        try:
            child.terminate()
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            print "terminate_process_tree:", "couldn't terminate child process with pid %s" % child.pid()
        else:
            child.wait(timeout=3)

    if including_parent:
        try:
            parent.terminate()
        except psutil.NoSuchProcess:
            pass
        except psutil.AccessDenied:
            print "terminate_process_tree:", "couldn't terminate parent process with pid %s" % parent.pid()
        else:
            parent.wait(timeout=3)

def terminate_child_processes():
    me = os.getpid()
    terminate_process_tree(me, including_parent=False)

#**************************************************************************
def rc_if_exists_and_done(ps):
    try:
        rc = ps.wait(0)
    except psutil.TimeoutExpired:
        rc = None
    return rc

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
    import subprocess
    ps = psutil.Popen(cmdList, stdin=None, stdout=subprocess.PIPE, stderr=errfd, **kwargs)
    comment = 'PID %d, stdout %s, stderr %s' % (
        ps.pid, os.path.basename(outpath), os.path.basename(errpath))
    print "spawn_cmd", cmd_string, comment

    # Reads the subprocess stdout until it is closed and 
    # ...echo it our python stdout and also the R stdout file in sandbox
    # Then wait for the program to exit. 
    # Read before wait so that you don't risk the pipe filling up and hanging the program. 
    # You wait after read for the final program exit and return code. 
    # If you don't wait, you'll get a zombie process (at least on linux)

    # this might not do what we want..see:
    # http://stackoverflow.com/questions/2804543/read-subprocess-stdout-line-by-line
    # I suppose we'll stop early?

    # shouldn't need a delay before checking this?
    if not ps.is_running():
        raise Exception("sh2junit: not immediate ps.is_running after start")

    # Not using Popen.communicate() or call() will result in a zombie process.
    # and it will always be "is_running"

    # A zombie process is not a real process
    # it's just a remaining entry in the process table until the parent process requests the child's return code. 
    # The actual process has ended and requires no other resources but said process table entry.
    linesMayExist = True
    while linesMayExist:
        # get whatever accumulated. only do up to 20 lines before we check timeout again
        wasRunning = ps.is_running()
        for lineBurstCnt in range(20):
            # print "lineBurstCnt:", lineBurstCnt
            # maybe I should use p.communicate() instead. have to keep it to stdout? or do stdout+stderr here
            line = ps.stdout.readline()
            if len(line)!=0:
                sys.stdout.write("R->" + line) # to our python stdout, with a prefix so it's obviously from R
                os.write(outfd, line) # to sandbox R stdout
            else:
                linesMayExist = wasRunning

        # Check. may have flipped to not running, and we just got the last bit.
        # shouldn't be a race on a transition here. if ps.wait(0) completion syncs the transition
        if wasRunning:
            print "ps.is_running():", ps.is_running(), ps.pid, ps.name, ps.status, ps.create_time
            # unload the return code without waiting..so we don't have a zombie!

        rc = rc_if_exists_and_done(ps)

        elapsed = time.time() - start
        # forever if timeout is None
        if timeout and elapsed > timeout:
            raise Exception("sh2junit: elapsed: %0.2f timeout: %s (secs) while echoing stdout from subprocess" % (elapsed, timeout))
        time.sleep(0.25)
        
    # It shouldn't be running now?

    # timeout=None waits forever. timeout=0 returns immediately.
    # default above is 5 minutes
    # Wait for process termination. Since child:  return the exit code. 
    # If the process is already terminated does not raise NoSuchProcess exception 
    # but just return None immediately. 
    # If timeout is specified and process is still alive raises psutil.TimeoutExpired() exception. 
    # old
    # rc = ps.wait(timeout)
    rc = ps.wait(3)
    elapsed = time.time() - start

    # FIX! should we check whether the # of lines in err is 0, or assume the rc tells us?
    print "rc:", rc
    if rc is None:
        # None means it already completed? 
        # FIX! Is it none if we get a timeout exception on this python ..how is that captured?
        errors = 0
    elif rc == 0:
        errors = 0
    else:
        errors = 1

    # Prune h2o logs to interesting lines and detect errors.
    # Error lines are returned. warning/info are printed to our (python stdout)
    # so that's always printed/saved?
    # None if no error
    sandboxErrorMessage = h2o_sandbox.check_sandbox_for_errors(
        LOG_DIR='./sandbox', 
        python_test_name=name, 
        cloudShutdownIsError=True, 
        sandboxIgnoreErrors=True) # don't take exception on error

    if sandboxErrorMessage:
        errors += 1

    out = file(outpath).read()
    err = file(errpath).read()
    create_junit_xml(name, out, err, sandboxErrorMessage, errors=rc, elapsed=elapsed)

    if not (rc or errors):
        return (errors, outpath, errpath)
    else:
        # dump all the info as part of the exception? maybe too much
        # is this bad to do in all cases? do we need it? 
        if ps.is_running():
            print "Before terminate:", ps.pid, ps.is_running()
            terminate_process_tree(ps.pid, including_parent=True)
        # could have already terminated?
        if rc is None:
            # ps.terminate()
            raise Exception("%s %s timed out after %d\nstdout:\n%s\n\nstderr:\n%s" %
                (name, cmd_string, timeout or 0, out, err))
        elif rc != 0:
            raise Exception("%s %s failed. errors: %s\nstdout:\n%s\n\nstderr:\n%s" % 
                (name, cmd_string, errors, out, err))
        else:
            raise Exception("%s %s has errors %s in ./sandbox log files?.\nstdout:\n%s\n\nstderr:\n%s" % 
                (name, cmd_string, errors, out, err))


#**************************************************************************
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-name', type=str, default='NoName', help='used to help name the xml/stdout/stderr logs created')
    parser.add_argument('-timeout', type=int, default=5, help='secs timeout for the shell subprocess. Fail if timeout')
    parser.add_argument('-cmd', '--cmd_string', type=str, default=None, help="cmd string to pass to shell subprocess. Better to just use'--' to start the cmd (everything after that is sucked in)")
    parser.add_argument('Rargs', nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if args.cmd_string:
        cmd_string = args.cmd_string
    else:
        # easiest way to handle multiple tokens for command
        # end with -- and this grabs the rest
        # drop the leading '--' if we stopped parsing the rest that way
        if args.Rargs:
            print "args.Rargs:", args.Rargs
            if args.Rargs[0]=='--':
                args.Rargs[0] = ''
            cmd_string = ' '.join(args.Rargs)
        else:
            # placeholder for test
            cmd_string = '/bin/ls'
        
    sh2junit(name=args.name, cmd_string=cmd_string, timeout=args.timeout)

