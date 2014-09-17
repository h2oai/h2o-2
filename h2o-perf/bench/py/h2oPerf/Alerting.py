import abc
import MySQLdb
import requests
import os
from datetime import datetime, timedelta
from LMSAdaptiveFilter import LMSAdaptiveFilter


# Global queries used throughout

TEST_NAMES_QUERY = \
    """
SELECT DISTINCT tr.test_name
FROM test_run tr
INNER JOIN test_run_phase_result tp
USING(test_run_id)
WHERE tp.end_epoch_ms >= {}
AND tp.phase_name = 'model';
"""

MOST_RECENTLY_RUN_TEST_NAME = \
    """
SELECT build_version
FROM test_run
WHERE test_name = '{}'
ORDER BY build_version DESC
LIMIT 1;
"""

CONTAMINATED = \
    """
SELECT contaminated
FROM test_run
WHERE test_name = '{}'
ORDER BY build_version DESC
"""

MULTIPLE_IDS = \
    """
SELECT tr.test_run_id, COUNT(*) cnt
FROM test_run tr
INNER JOIN test_run_phase_result tp
USING(test_run_id)
WHERE tp.phase_name = 'model'
AND tr.build_version LIKE '%{}%'
AND tr.test_name = '{}'
GROUP BY tr.test_run_id
HAVING cnt > 1;
"""

CORRECT = \
    """
SELECT correctness_passed
FROM test_run
WHERE test_name = '{}'
ORDER BY build_version DESC;
"""

TIMING = \
    """
SELECT (tp.end_epoch_ms - tp.start_epoch_ms) / 1000 elapsed
FROM test_run tr
INNER JOIN test_run_phase_result tp
USING (test_run_id)
WHERE tr.timing_passed = 1
AND tr.test_name = '{}'
ORDER BY tr.start_epoch_ms DESC
LIMIT {};
"""

# A dictionary of the queries appearing above
QUERIES = {
    "test_names": TEST_NAMES_QUERY,
    "test_build_num": MOST_RECENTLY_RUN_TEST_NAME,
    "contaminated": CONTAMINATED,
    "multiple_ids": MULTIPLE_IDS,
    "correct": CORRECT,
    "timing": TIMING,
}

CORRECT_ALERT_HEADER = \
    """
Correctness Alerts
------------------
"""

TIMING_ALERT_HEADER = \
    """
Timing Alerts
-------------
"""

INFRASTRUCTURE_ALERT_HEADER = \
    """
Infrastructure Alerts
---------------------
"""


class Alert:
    """
    The Alert class.

    This is an abstract class whose subclasses contain methods and state for sending out email alerts
    when a performance test fails.

    There are three flavors of failure for which there is alerting:
      1. Speed related.
      2. Correctness related.
      3. Infrastructure related.

    The following sets of conditions must be satisfied in order for an alert to be sent out:

      1. Speed related:
          Condition 1: No multiple test IDs for the same run phase and build number (infrastructure problem)
          Condition 2: Test is not contaminated
          Condition 3: The run time of the test phase is detected by the LMS adaptive filter.

      2. Correctness related:
          Condition 1: No multiple test IDs for the same run phase and build number (infrastructure problem)
          Condition 2: Test is not contaminated
          Condition 3: The correct field for the test_run is FALSE or 0

      3. Infrastructure related:
          Condition 1: Multiple test IDs for the same phase and build number.
          Condition 2: Test did not run/complete.

        NB: If the build fails, Jenkins already alerts spencer@0xdata.com

    Developer Note:
        Private methods (those that begin with '_') are to be used for performing queries of the MySQL database PerfDB.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, order):
        """
        Every Alert object will have a list of test names that have runs from the last N days.
        :param order: The order is the number of days back to look back (this is the `N` above).
        :return:
        """
        self.order = order

        # Setup a connection to the db
        self.host = 'mr-0x1'
        self.db = MySQLdb.connect(host=self.host,
                                  user="spencer",
                                  passwd="spencer",
                                  db="PerfDB",
                                  port=3306)
        self.cursor = self.db.cursor()

        # A list of test names from the last `order` days
        self.test_names = self._get_test_names()

        # A dictionary of tests to alert on and messages to alert with
        self.alert_list = {}

    @abc.abstractmethod
    def should_alert(self, test_name):
        """
        Retrieve run data from PerfDB for this test_name. If no recent data available,
        then create and send infrastructure alert.

        Recent data means: Build number matches current build number from master.
        """
        return

    def is_recent(self, test_name):
        cur_bn = Alert._get_build_number('master')
        test_bn = self._get_test_build_number(test_name)
        return cur_bn == test_bn

    def was_contaminated(self, test_name):
        """
        Check the most recent run of this test_name.
        If not recent, returns "NA".

        Expect that the InfrastructureAlert object will handle the alerting for inconsistent build numbers.
        """
        if self.is_recent(test_name):
            return self._check_contaminated(test_name)
        return False

    def has_multiple_ids(self, test_name):
        """
        Check if the test_name has multiple IDs.
        If not recent, returns "NA".
        """
        if self.is_recent(test_name):
            return self._multiple_ids_helper(test_name)
        return False

    def add_to_alert_list(self, test_name, message):
        self.alert_list[test_name] = message

    def _multiple_ids_helper(self, test_name):
        test_build_number = self._get_test_build_number(test_name, True).strip('"')
        query = QUERIES["multiple_ids"].format(test_build_number, test_name.strip('"'))
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        if len(res) != 0:
            return True
        return False

    def _check_contaminated(self, test_name):
        query = QUERIES["contaminated"].format(test_name.strip('"'))
        self.cursor.execute(query)
        res = self.cursor.fetchone()
        return res[0] == 0

    def _get_test_build_number(self, test_name, full=False):
        query = QUERIES["test_build_num"].format(test_name.strip('"'))
        self.cursor.execute(query)
        bn = self.cursor.fetchone()
        if full:
            return bn[0].strip()
        return bn[0].strip().split('.')[-1]

    def _get_test_names(self):
        epoch = datetime.utcfromtimestamp(0)
        dt = datetime.now()
        dt2 = dt - timedelta(self.order)
        reference_time_millis = (dt2 - epoch).total_seconds() * 1000
        test_names_query = QUERIES["test_names"].format(reference_time_millis)
        self.cursor.execute(test_names_query)
        test_names = self.cursor.fetchall()
        return [test_names[i][0] for i in range(len(test_names))]

    @staticmethod
    def _get_build_number(branch):
        build_number = requests.get("http://s3.amazonaws.com/h2o-release/h2o/" + branch + "/latest").text
        return str(build_number.strip())


class CorrectAlert(Alert):
    """
    This class is responsible for sending out alerts when a test fails its correctness criteria.

    The correctness of each test is stored in the `test_run` table under the column `correctness_passed`, which
    is a boolean:
        0: Incorrect
        1: Correct
    """

    def __init__(self, order):
        super(CorrectAlert, self).__init__(order)

    def should_alert(self, test_name):
        if not self.was_contaminated(test_name) \
                and not self.has_multiple_ids(test_name) \
                and self.is_recent(test_name):
            return self._is_correct(test_name)
        return False

    def _is_correct(self, test_name):
        query = QUERIES["correct"].format(test_name.strip('"'))
        self.cursor.execute(query)
        res = self.cursor.fetchone()
        return res[0] == 0  # 1: Correct, 0: Incorrect


class SpeedAlert(Alert):
    """
    This class is responsible for sending out alerts when a test fails its timing criteria.

    Unlike correctness alerts based on how long the test took to run are based on an outlier detector. Here we use the
    LMS adaptive filter ( *which additionally implements the exclusion of outliers**) to detect test run times that are
    out of the ordinary.

    This is where the `order`, or in time-series parlance `lag` (or `lag order`) comes in to play. This is just the
    number of previous data points we want to include in our evaluation of the new data point. If the incoming point is
    "OK" then nothing happens and it does not update the `timing_passed` field in the `test_run` table. If it is
    determined to be an outlier, the `timing_passed` field switches from 1 -> 0. All points with a `timing_passed` value
    of 0 are excluded from future computations (as we do not wish to contaminate the running statistics by including
    spurious results).
    """

    def __init__(self, order):
        super(SpeedAlert, self).__init__(order)

    def should_alert(self, test_name):
        if not self.was_contaminated(test_name) \
                and not self.has_multiple_ids(test_name) \
                and self.is_recent(test_name):
            return self._is_ontime(test_name)
        return False

    def _is_ontime(self, test_name):
        """
        The input stream is an incoming stream of elapsed times from the last `order` runs of the given test_name.
        The input stream is initially sorted by most recent to furthest back in time. Therefore, exclude the first
        entry, and perform the LMS on the next `order - 1` data points.
        """
        input_stream = self._get_input_stream(test_name)
        if input_stream == "NA": return False  # This condition should never happen
        if len(input_stream) == 1: return True  # Only have a single data point, nothing to compute.

        query_point = input_stream[0]
        data_points = input_stream[1:]
        fil = LMSAdaptiveFilter(len(data_points))
        for t in data_points:
            fil.X.add(t)
        return fil.is_signal_outlier(query_point)

    def _get_input_stream(self, test_name):
        query = QUERIES["timing"].format(test_name.strip('"'), self.order)
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        if len(res) == 0:
            return "NA"
        if len(res) == 1:
            return [int(res[0])]
        if len(res) > 1:
            return [int(res[i][0]) for i in range(len(res))]


class InfrastructureAlert(Alert):
    """
    This class is responsible for sending out alerts when a test fails for reasons other than speed and correctness.
    """

    def __init__(self, order):
        super(InfrastructureAlert, self).__init__(order)

    def should_alert(self, test_name):
        return not self.is_recent(test_name)


class Alerter:
    """
    The Alerter class.

    This class manages the various types of alerts that may occur. In addition, this class handles the actual
    alerting by email.
    """

    def __init__(self, order, names):
        self.correct_alert = CorrectAlert(order)
        self.speed_alert = SpeedAlert(order)
        self.infrastructure_alert = InfrastructureAlert(order)
        self.test_list = names

        self.test_names = self.correct_alert.test_names  # `correct_alert` chosen WLOG

    def alert(self):
        self._gather_alerts()
        self._do_alert()

    def _gather_alerts(self):
        for name in self.test_names:
            if name not in self.test_list: continue
            if self.correct_alert.should_alert(name):
                self.correct_alert.add_to_alert_list(name, "Failed correctness.")

            if self.speed_alert.should_alert(name):
                self.speed_alert.add_to_alert_list(name, "Failed timing.")

            if self.infrastructure_alert.should_alert(name):
                self.infrastructure_alert.add_to_alert_list(name, "Test failed to run.")

        for name in self.test_list:
            if name not in self.test_names:
                if name not in self.infrastructure_alert.alert_list:
                    self.infrastructure_alert.add_to_alert_list(name, "Test failed to run.")

    def _do_alert(self):
        this_path = os.path.dirname(os.path.realpath(__file__))
        res_path = os.path.join(this_path, '..', "results", "Alerts.txt")
        with open(res_path, 'w') as f:

            # Check & Report Correctness Alerts
            f.write(CORRECT_ALERT_HEADER)
            f.write('\n')
            if len(self.correct_alert.alert_list) > 0:
                for key in self.correct_alert.alert_list:
                    f.write("FAIL		" + key + " failed:  " + self.correct_alert.alert_list[key])
                    f.write('\n')
            else:
                f.write("All tests were correct.")
                f.write("\n")

            # Check & Report Timing Alerts
            f.write(TIMING_ALERT_HEADER)
            f.write('\n')
            if len(self.speed_alert.alert_list) > 0:
                for key in self.speed_alert.alert_list:
                    f.write("FAIL		" + key + " failed:  " + self.speed_alert.alert_list[key])
                    f.write('\n')
            else:
                f.write("No tests failed due to untimeliness.")
                f.write("\n")

            # Check & Report Infrastructure Alerts
            f.write(INFRASTRUCTURE_ALERT_HEADER)
            f.write('\n')
            if len(self.infrastructure_alert.alert_list) > 0:
                for key in self.infrastructure_alert.alert_list:
                    f.write("FAIL		" + key + " failed:  " + self.infrastructure_alert.alert_list[key])
                    f.write('\n')
            else:
                f.write("All tests ran.")
                f.write("\n")

