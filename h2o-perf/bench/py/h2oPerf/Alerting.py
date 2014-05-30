import abc



class Alerter:
    """
    The Alerter class.

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
          Condition 3: Test must actually have completed
          Condition 4: The Test ran longer than 2 standard deviations above the mean of the last 5 runs

      2. Correctness related:
          Condition 1: No multiple test IDs for the same run phase and build number (infrastructure problem)
          Condition 2: Test is not contaminated
          Condition 3: The correct field for the test_run is FALSE or 0

      3. Infrastructure related:
          Condition 1: Multiple test IDs for the same phase and build number.
          Condition 2: Test did not run/complete.
          **NB: If the build fails, Jenkins already alerts spencer@0xdata.com
      """

      __metacalss__ = abc.ABCMeta

      @abc.abstractmethod
      def should_alert(self, test_name):
          """
          Retrieve run data from PerfDB for this test_name. If no recent data available,
          then create and send infrastructure alert.
          
          Recent data means: Build number matches current build number from master.
          """
          return

      def is_recent

class CorrectAlert(Alerter):
    


class SpeedAlert(Alerter):

