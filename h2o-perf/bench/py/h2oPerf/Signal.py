from math import sqrt


class Signal:
    """
    A class that represents a signal lagged by amount 'order'.

    A signal's 'order' is how much of its history is tracked.
    This is also known as the lag.

    The first element in the signal list is the most recent value of the signal.
    """

    def __init__(self, order):
        self.signal = []
        self.order = order

    def add(self, sig):
        """
        Prepend an incoming signal value to the signal history.
        """
        if self.length() < self.order:
            self.signal.insert(0, sig)
        else:
            self.pop()
            self.signal.insert(0, sig)

    def pop(self):
        """
        Pop the last element of the list.
        """
        del self.signal[-1]

    def length(self):
        """
        Remove the last element of the list.
        """
        return len(self.signal)

    def report(self):
        """
        A debugging function to report information on the signal.
        """
        print str(self.signal), str(self.can_use())

    def can_use(self):
        """
        A signal may be used once it's length is equal to its order.
        """
        return len(self.signal) == self.order

    def mean(self):
        """
        Compute the mean of the signal.
        """
        return sum(self.signal) * 1. / (1. * self.length())

    def sigma(self):
        """
        Compute the standard deviation of the signal.
        """
        ybar = self.mean()
        num = sum([(y - ybar) ** 2 for y in self.signal]) * 1.0
        denom = self.length() * 1.
        if denom <= 0:
            raise Exception("DIVISION BY ZERO")
        return sqrt(num / denom)

    @staticmethod
    def run_test():
        """
        A unit test for the Signal class.
        """
        x = range(20)
        x2 = [0.123, 232.13, 21938, 2341, 214, 124, 214, 9291, 12313, 8237.123, 23, 14, 21, 3, 4, 6, 7, 8, 1293, 10191]

        s1 = Signal(5)
        s2 = Signal(1)

        for i in x[::-1]:
            s1.add(i)
            s1.report()

        for i in x2[::-1]:
            s2.add(i)
            s2.report()