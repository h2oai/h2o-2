from Signal import Signal
import numpy as np


class LMSAdaptiveFilter:
    """ 
    The LMS Adaptive Filter.
    """

    def __init__(self, order, damping=0.5):
        self.order = order
        self.damping = damping
        self.X = Signal(order)
        self.Y = Signal(order)
        self.weights = [0] * order

    def is_signal_outlier(self, sig):
        X = np.array(self.X.signal)
        weights = np.array(self.weights)
        yest = weights.dot(X)
        c = (1.0 * (sig - yest)) / (1. * X.dot(X))
        weights = weights + self.damping * c * X
        self.X.add(sig)
        self.weights = list(weights)
        return self._check_est(yest)

    def _check_est(self, est):
        if self.Y.can_use():
            return est >= (2.0 * self.Y.sigma() + self.Y.mean())
        return False