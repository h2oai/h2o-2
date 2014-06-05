import abc
import MySQLdb
import requests
from datetime import datetime, timedelta


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

# A dictionary of the queries appearing above
QUERIES = {
    "test_names": TEST_NAMES_QUERY,
    "test_build_num": MOST_RECENTLY_RUN_TEST_NAME,
    "contaminated": CONTAMINATED,
    "multiple_ids": MULTIPLE_IDS,
}


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
        self.host = '192.168.1.171'
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
        return "NA"

    def has_multiple_ids(self, test_name):
        """
        Check if the test_name has multiple IDs.
        If not recent, returns "NA".
        """
        if self.is_recent(test_name):
            return self._multiple_ids_helper(test_name)
        return "NA"

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
        epoch = datetime.datetime.utcfromtimestamp(0)
        dt = datetime.now()
        dt2 = dt - timedelta(self.order)
        reference_time_millis = (dt2 - epoch).total_seconds() * 1000
        test_names_query = QUERIES["test_names"].format(reference_time_millis)
        self.cursor.execute(test_names_query)
        test_names = self.cursor.fetchall()
        return [test_names[i][0] for i in range(len(test_names))]

    @staticmethod
    def _get_build_number(branch):
        build_number = requests.get("http://s3.amazonaws.com/h2o-release/h2o/" + branch + "/latest")
        return str(build_number.strip())


class CorrectAlert(Alert):
    """
    This class is responsible for sending out alerts when a test fails its correctness criteria.
    """

    def __init__(self, order):
        super(CorrectAlert, self).__init__(order)

    def should_alert(self, test_name):
        pass


class SpeedAlert(Alert):
    """
    This class is responsible for sending out alerts when a test fails its timing criteria.
    """

    def __init__(self, order):
        super(SpeedAlert, self).__init__(order)

    def should_alert(self, test_name):
        pass


class InfrastructureAlert(Alert):
    """
    This class is responsible for sending out alerts when a test fails for reasons other than speed and correctness.
    """

    def __init__(self, order):
        super(InfrastructureAlert, self).__init__(order)

    def should_alert(self, test_name):
        pass


class Alerter:
    """
    The Alerter class.

    This class manages the various types of alerts that may occur. In addition, this class handles the actual
    alerting by email.
    """
    def __init__(self, order):
        self.correct_alert = CorrectAlert(order)
        self.speed_alert = SpeedAlert(order)
        self.infrastructure_alert = InfrastructureAlert(order)

        # This is a list of tests with a "modeling" phase
        self.test_list = ['singlenode_deeplearning_mnist'
                          'multinode_deeplearning_mnist'
                          'singlenode_glm_va_airlines'
                          'singlenode_pca_one-billion-rows'
                          'singlenode_kmeans_va_airlines'
                          'singlenode_summary_one-billion-rows'
                          'singlenode_rf_va_mnist'
                          'singlenode_summary_va_airlines'
                          'singlenode_rf_fv_mnist'
                          'singlenode_pca_airlines'
                          'singlenode_ddply_airlines-1B'
                          'singlenode_kmeans_fv_one-billion-rows'
                          'singlenode_glm_fv_one-billion-rows'
                          'singlenode_kmeans_one-billion-rows'
                          'singlenode_gbm_covtype'
                          'singlenode_glm_one-billion-rows'
                          'multinode_kmeans_va_airlines'
                          'multinode_rf_va_mnist'
                          'multinode_summary_va_airlines'
                          'multinode_rf_fv_mnist'
                          'multinode_pca_airlines'
                          'multinode_gbm_covtype'
                          'singlenode_deeplearning_multinomial_correctness_mnist'
                          'multinode_summary_one-billion-rows'
                          'multinode_pca_one-billion-rows'
                          'multinode_kmeans_fv_one-billion-rows'
                          'multinode_glm_fv_one-billion-rows'
                          'multinode_kmeans_one-billion-rows'
                          'multinode_glm_one-billion-rows']

        self.test_names = self.correct_alert.test_names  # correct_alert chosen WLOG

    def gather_alerts(self):

