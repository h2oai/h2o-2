#!/usr/bin/python
import sys, itertools, os, re

def check_sandbox_for_errors(LOG_DIR=None, python_test_name='',
    cloudShutdownIsError=False, sandboxIgnoreErrors=False):
    # show the parameters
    ### print "check_sandbox_for_errors:", locals()
    
    # gets set below on error (returned)
    errorFound = False

    if not LOG_DIR:
        LOG_DIR = './sandbox'

    if not os.path.exists(LOG_DIR):
        return

    # FIX! wait for h2o to flush to files? how?
    # Dump any assertion or error line to the screen
    # Both "passing" and failing tests??? I guess that's good.
    # if you find a problem, just keep printing till the end, in that file.
    # The stdout/stderr is shared for the entire cloud session?
    # so don't want to dump it multiple times?
    errLines = []
    for filename in os.listdir(LOG_DIR):
        # don't search the R stdout/stderr
        # this matches the python h2o captured stdout/stderr, and also any downloaded h2o logs
        # not the commands.log
        if re.search('h2o.*stdout|h2o.*stderr',filename) and not re.search('doneToLine',filename):
            sandFile = open(LOG_DIR + "/" + filename, "r")

            # if we've already walked it, there will be a matching file
            # with the last line number we checked
            try:
                with open(LOG_DIR + "/" + "doneToLine." + filename) as f:
                    doneToLine = int(f.readline().rstrip())
            except IOError:
               # no file
               doneToLine = 0

            # just in case error/assert is lower or upper case
            # FIX! aren't we going to get the cloud building info failure messages
            # oh well...if so ..it's a bug! "killing" is temp to detect jar mismatch error
            regex1String = 'found multiple|exception|error|ERRR|assert|killing|killed|required ports'
            if cloudShutdownIsError:
                regex1String += '|shutdown command'
            regex1 = re.compile(regex1String, re.IGNORECASE)
            regex2 = re.compile('Caused',re.IGNORECASE)
            # regex3 = re.compile('warn|info|TCP', re.IGNORECASE)
            # FIX! temp to avoid the INFO in jan's latest logging. don't print any info?
            regex3 = re.compile('warn|TCP', re.IGNORECASE)

            # many hdfs/apache messages have 'error' in the text. treat as warning if they have '[WARN]'
            # i.e. they start with:
            # [WARN]

            # if we started due to "warning" ...then if we hit exception, we don't want to stop
            # we want that to act like a new beginning. Maybe just treat "warning" and "info" as
            # single line events? that's better
            printing = 0 # "printing" is per file.
            lines = 0 # count per file! errLines accumulates for multiple files.
            currentLine = 0
            log_python_test_name = None
            for line in sandFile:
                currentLine += 1

                m = re.search('(python_test_name:) (.*)', line)
                if m:
                    log_python_test_name = m.group(2)
                    # if log_python_test_name == python_test_name):
                    #    print "Found log_python_test_name:", log_python_test_name

                # don't check if we've already checked
                if currentLine <= doneToLine:
                    continue

                # if log_python_test_name and (log_python_test_name != python_test_name):
                #     print "h2o_sandbox.py: ignoring because wrong test name:", currentLine

                # JIT reporting looks like this..don't detect that as an error
                printSingleWarning = False
                foundBad = False
                if not ' bytes)' in line:
                    # no multiline FSM on this
                    printSingleWarning = regex3.search(line)
                    #   13190  280      ###        sun.nio.ch.DatagramChannelImpl::ensureOpen (16 bytes)
                    # don't detect these class loader info messags as errors
                    #[Loaded java.lang.Error from /usr/lib/jvm/java-7-oracle/jre/lib/rt.jar]
                    foundBad = regex1.search(line) and not (
                        ('Error on training data' in line) or
                        ('Error on validation data' in line) or
                        ('Act/Prd' in line) or
                        ('water.DException' in line) or
                        # the manyfiles data has eRRr in a warning about test/train data
                        ('WARN SCORM' in line) or
                        # ignore the long, long lines that the JStack prints as INFO
                        ('stack_traces' in line) or
                        # shows up as param to url for h2o
                        ('out_of_bag_error_estimate' in line) or
                        # R stdout confusion matrix. Probably need to figure out how to exclude R logs
                        ('Training Error' in line) or
                        # now from GBM
                        ('Mean Squared Error' in line) or
                        ('Error' in line and 'Actual' in line) or
                        # fvec
                        ('prediction error' in line) or ('errors on' in line) or
                        # R
                        ('class.error' in line) or
                        # original RF
                        ('error rate' in line) or ('[Loaded ' in line) or
                        ('[WARN]' in line) or ('CalcSquareErrorsTasks' in line))

                if (printing==0 and foundBad):
                    printing = 1
                    lines = 1
                elif (printing==1):
                    lines += 1
                    # if we've been printing, stop when you get to another error
                    # keep printing if the pattern match for the condition
                    # is on a line with "Caused" in it ("Caused by")
                    # only use caused for overriding an end condition
                    foundCaused = regex2.search(line)
                    # since the "at ..." lines may have the "bad words" in them, we also don't want
                    # to stop if a line has " *at " at the beginning.
                    # Update: Assertion can be followed by Exception.
                    # Make sure we keep printing for a min of 4 lines
                    foundAt = re.match(r'[\t ]+at ',line)
                    if foundBad and (lines>10) and not (foundCaused or foundAt):
                        printing = 2

                if (printing==1):
                    # to avoid extra newline from print. line already has one
                    errLines.append(line)
                    sys.stdout.write(line)

                if (printSingleWarning):
                    # don't print these lines
                    if not (
                        ('Unable to load native-hadoop library' in line) or
                        ('stack_traces' in line) or
                        ('Multiple local IPs detected' in line) or
                        ('[Loaded ' in line) or
                        ('RestS3Service' in line) ):
                        sys.stdout.write(line)

            sandFile.close()
            # remember what you've checked so far, with a file that matches, plus a suffix
            # this is for the case of multiple tests sharing the same log files
            # only want the test that caused the error to report it. (not flat the subsequent ones as fail)
            # overwrite if exists
            with open(LOG_DIR + "/" + "doneToLine." + filename, "w") as f:
                f.write(str(currentLine) + "\n")

    sys.stdout.flush()

    # already has \n in each line
    # doing this kludge to put multiple line message in the python traceback,
    # so it will be reported by jenkins. The problem with printing it to stdout
    # is that we're in the tearDown class, and jenkins won't have this captured until
    # after it thinks the test is done (tearDown is separate from the test)
    # we probably could have a tearDown with the test rather than the class, but we
    # would have to update all tests.
    if len(errLines)!=0:
        # check if the lines all start with INFO: or have "apache" in them
        justInfo = True
        for e in errLines:
            justInfo &= re.match("INFO:", e) or ("apache" in e)

        if not justInfo:
            emsg1 = " check_sandbox_for_errors: Errors in sandbox stdout or stderr (or R stdout/stderr).\n" + \
                     "Could have occurred at any prior time\n\n"
            emsg2 = "".join(errLines)
            errorFound = True
            errorMessage = python_test_name + emsg1 + emsg2
            if not sandboxIgnoreErrors:
                raise Exception(errorMessage)

    if errorFound:
        return errorMessage
    else:
        return

if __name__ == "__main__":
    # if you call from the command line, we'll just pass the first two positionally.
    # here's a low budget argsparse :) (args are optional!)
    arg_names = ['me', 'LOG_DIR', 'python_test_name', 'cloudShutdownIsError', 'sandboxIgnoreErrors']
    args = dict(itertools.izip_longest(arg_names, sys.argv))
    errorMessage = check_sandbox_for_errors(
        LOG_DIR=args['LOG_DIR'], 
        python_test_name=args['python_test_name'],
        cloudShutdownIsError=args['cloudShutdownIsError'], 
        sandboxIgnoreErrors=args['sandboxIgnoreErrors'])

    # it shouldn't return here because it should take the exception)
    if errorMessage:
        raise Exception('Error found in the logs that we want to consider fatal')

