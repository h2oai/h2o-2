from Scrape import *
from Table import *
import re
import os
import subprocess
import time
import atexit

class Process:
    """
    @param test_dir: Full absolute path to the test directory.
    @param test_short_dir: Path from h2o/R/tests to the test directory.
    @param output_dir: The directory where we can create an output file for this process.
    @return: The test object.
    """
    def __init__(self, test_dir, test_short_dir, output_dir):
        self.test_dir = test_dir
        self.test_short_dir = test_short_dir
        self.output_dir = output_dir
        self.test_name = ""
        self.output_file_name = ""

        self.canceled = False
        self.terminated = False
        self.returncode = None #self.__did_complete__()
        self.ip = None
        self.pid = -1
        self.port = None
        self.port = None
        self.child = None

    def poll(self):
        """
        Poll to see if process completed.
        """
        return self.__did_complete__()

    def cancel(self):
        """
        Cancel a process.
        """
        if (self.pid <= 0):
            self.canceled = True

    def terminate(self):
        """
        Terminate the process. (Due to a signal.)
        """
        self.terminated = True
        if (self.pid > 0):
            print("Killing R process with PID {}".format(self.pid))
            try:
                self.child.terminate()
            except OSError:
                pass
        self.pid = -1

    def get_test_dir_file_name(self):
        """  
        @return: The full absolute path of this test.
        """
        return os.path.join(self.test_dir, self.test_name)

    def get_test_name(self):
        """  
        @return: The file name (no directory) of this test.
        """
        return self.test_name

    def get_ip(self):
        """  
        @return: IP of the cloud where this test ran.
        """
        return self.ip

    def get_port(self):
        """  
        @return: Integer port number of the cloud where this test ran.
        """
        return int(self.port)

    def get_passed(self):
        """  
        @return: True if the test passed, False otherwise.
        """
        return (self.returncode == 0)

    def get_completed(self):
        """  
        @return: True if the test completed (pass or fail), False otherwise.
        """
        return (self.returncode > self.__did_not_complete__())

    def get_output_dir_file_name(self):
        """  
        @return: Full path to the output file which you can paste to a terminal window.
        """
        return (os.path.join(self.output_dir, self.output_file_name))

    def __str__(self):
        s = ""
        s += "Teste: {}/{}\n".format(self.test_dir, self.test_name)
        return s

    def __did_complete__(self):
        """
        Check if a R subprocess finished.
        """
        child = self.child
        if (child is None):
            return False
        child.poll()
        if (child.returncode is None):
            return False
        self.pid = -1
        self.returncode = child.returncode
        return True

    def __did_not_complete__(self):
        """
        returncode marker to know if test ran or not.
        """
        return -9999999

class RProc(Process):
    """ 
    This class represents a connection to an R subprocess.
    @param rfile: This is the name of the R file that is 
                  to be subproccessed. Example: gbm_test1_Parse.R
    """
    def __init__(self, test_dir, test_short_dir, output_dir, rfile, perfdb):
        self.perfdb = perfdb
        self.rfile = rfile
        self.rtype = self.__get_type__()
        self.test_dir = test_dir
        self.test_short_dir = test_short_dir
        self.output_dir = output_dir
        self.test_name = self.rfile
        self.output_file_name = ""
        self.did_time_pass = 0
        self.did_correct_pass = 0
        self.contaminated = 0
        self.contamination_message = ""

        self.canceled = False
        self.terminated = False
        self.returncode = None
        self.ip = None
        self.pid = -1
        self.port = None
        self.port = None
        self.child = None

    def start(self, ip, port):
        """ 
        Start an R subproccess.
        """
        self.ip = ip
        self.port = port
     
        print 
        print "DEBUG RPROCESS: "
        print "TEST NAME: " + self.test_name
        print
        print "RFILE : " + self.rfile
        print

        cmd = ["R", "-f", self.rfile, "--args", self.ip + ":" + str(self.port)]
        short_dir = re.sub(r'[\\/]', "_", self.test_short_dir)
        self.output_file_name = os.path.join(self.output_dir,
                                             short_dir + "_" + self.test_name + ".out")

        print "DEBUG PROCESS OUT FILE NAME: "
        print "OUT FILE NAME: " + self.output_file_name

        f = open(self.output_file_name, "w")

        try:
            self.child = subprocess.Popen(args=cmd,
                                          stdout=f,
                                          stderr=subprocess.STDOUT,
                                          cwd=self.test_dir)
            f.close()
        except:
            from PerfUtils import dash_line
            print dash_line
            "PHASE FAILED!"
            "Last 10 lines of the output file:"
            f.close()
            o = open(self.output_file_name, 'r')
            print(tail(o,10))
            print dash_line
            raise(Exception("Phase failure: " + (self.rfile).strip('.R')))



        @atexit.register
        def kill_process():
            try:
                self.child.terminate()
            except OSError:
                pass
        self.pid = self.child.pid

    def scrape_phase(self):
        scraper = Scraper(self.perfdb, self.rtype[0], self.test_dir, self.test_short_dir, self.output_dir, self.output_file_name)
        res = scraper.scrape()
        self.contaminated = scraper.contaminated
        self.contamination_message = scraper.contamination_message
        self.did_time_pass = scraper.did_time_pass
        self.did_correct_pass = scraper.did_correct_pass
        return res

    def block(self):
        while(True):
            if self.terminated:
                return None
            if self.poll():
                break
            time.sleep(1)

    def __get_type__(self):
        """ 
        Returns the type: 'parse', 'model', 'predict'
        """
        types = ['parse', 'model', 'predict']
        rf = self.rfile.lower()
        return [t for t in types if t in rf]

    @staticmethod
    def tail( f, window=20 ):
        BUFSIZ = 1024
        f.seek(0, 2)
        bytes = f.tell()
        size = window
        block = -1
        data = []
        while size > 0 and bytes > 0:
            if (bytes - BUFSIZ > 0):
                # Seek back one whole BUFSIZ
                f.seek(block*BUFSIZ, 2)
                # read BUFFER
                data.append(f.read(BUFSIZ))
            else:
                # file too small, start from begining
                f.seek(0,0)
                # only read what was not read
                data.append(f.read(bytes))
            linesFound = data[-1].count('\n')
            size -= linesFound
            bytes -= BUFSIZ
            block -= 1
        return '\n'.join(''.join(data).splitlines()[-window:])
