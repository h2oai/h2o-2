import sys
import os
import shutil
import threading
import stat
import tempfile
from math import sqrt

sig = False
dash_line = "\n-------------------------------------------------------------------------------------\n"


def use(x):
    """ Hack to remove compiler warning. """
    if False:
        print(x)


def signal_handler(signum, stackframe):
    global sig
    global dash_line
    use(stackframe)
    if sig:
        # Don't do this recursively.
        return
    sig = True
    print dash_line
    print("SIGNAL CAUGHT (" + str(signum) + ").  STOPPING TESTS AND ROLLING BACK TRANSACTIONS.")
    print dash_line
    sys.exit(1)


def wipe_output_dir(dir_to_wipe):
    print("")
    print("Wiping output directory...")
    try:
        if (os.path.exists(dir_to_wipe)):
            shutil.rmtree(dir_to_wipe)
    except OSError as e:
        print("")
        print("ERROR: Removing output directory failed: " + dir_to_wipe)
        print("       (errno {0}): {1}".format(e.errno, e.strerror))
        print("")
        sys.exit(1)


def start_cloud(object, use_remote):
    """ 
    Start H2O Cloud
    """
    if (object.terminated):
        return

    print("")
    print("Starting cloud...")
    print("")

    if (object.terminated):
        return

    if use_remote:
        object.cloud[0].start_remote()
    else:
        object.cloud[0].start_local()

    print("")
    print("Waiting for H2O nodes to come up...")
    print("")

    if (object.terminated):
        return
    object.cloud[0].wait_for_cloud_to_be_up()
    object.jvm_output_file = object.cloud[0].nodes[0].get_output_file_name()


def run_contaminated(object):
    """
    Check if the run was contaminated.
    """

    if object.terminated:
        return

    return object.cloud[0].check_contaminated()


def stop_cloud(object, use_remote):
    """
    Stop H2O cloud.
    """
    if object.terminated:
        return

    # if object.use_cloud:
    #        print("")
    #        print("All tests completed...")
    #        print("")
    #        return

    print("")
    print("All tests completed; tearing down clouds...")
    print("")
    if use_remote:
        object.cloud[0].stop_remote()
    else:
        object.cloud[0].stop_local()


def __scrape_h2o_sys_info__(object):
    """
    Scrapes the following information from the jvm_output_file:
        user_name, build_version, build_branch, build_sha, build_date,
        cpus_per_host, heap_bytes_per_node
    """
    test_run_dict = {}
    test_run_dict['product_name'] = "h2o"
    test_run_dict['component_name'] = "None"
    with open(object.jvm_output_file, "r") as f:
        for line in f:
            line = line.replace('\n', '')
            if "Built by" in line:
                test_run_dict['user_name'] = line.split(': ')[-1]
            if "Build git branch" in line:
                test_run_dict['build_branch'] = line.split(': ')[-1]
            if "Build git hash" in line:
                test_run_dict['build_sha'] = line.split(': ')[-1]
            if "Build project version" in line:
                test_run_dict['build_version'] = line.split(': ')[-1]
            if "Built on" in line:
                test_run_dict['build_date'] = line.split(': ')[-1]
            if "Java availableProcessors" in line:
                test_run_dict['cpus_per_host'] = line.split(': ')[-1]
            if "Java heap maxMemory" in line:
                test_run_dict['heap_bytes_per_node'] = str(float(line.split(': ')[-1].split(' ')[0]) * 1024 * 1024)
            if "error" in line.lower():
                test_run_dict['error_message'] = line
            else:
                test_run_dict['error_message'] = "No error"
    return test_run_dict


def report_summary(object):
    """
    Report some summary information when the tests have finished running.

    @return: none
    """
    global dash_line
    passed = 0
    failed = 0
    notrun = 0
    total = 0
    for test in object.tests:
        if (test.get_passed()):
            passed += 1
        else:
            if (test.get_completed()):
                failed += 1
            else:
                notrun += 1
        total += 1
    end_seconds = time.time()
    delta_seconds = end_seconds - object.start_seconds
    run = total - notrun
    object.__log__(dash_line)
    object.__log__("SUMMARY OF RESULTS")
    object.__log__(dash_line)
    object.__log__("Total tests:          " + str(total))
    object.__log__("Passed:               " + str(passed))
    object.__log__("Did not pass:         " + str(failed))
    object.__log__("Did not complete:     " + str(notrun))
    object.__log__("")
    object.__log__("Total time:           %.2f sec" % delta_seconds)
    if (run > 0):
        object.__log__("Time/completed test:  %.2f sec" % (delta_seconds / run))
    else:
        object.__log__("Time/completed test:  N/A")
    object.__log__("")


def __drain__(src, dst):
    for l in src:
        if type(dst) == type(0):
            os.write(dst, l)
        else:
            dst.write(l)
            dst.flush()
    src.close()
    if type(dst) == type(0):
        os.close(dst)


def drain(src, dst):
    t = threading.Thread(target=__drain__, args=(src, dst))
    t.daemon = True
    t.start()


def tmp_file(prefix='', suffix='', directory=''):
    fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=directory)
    permissions = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH
    os.chmod(path, permissions)
    return (fd, path)

