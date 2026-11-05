
import numpy as np


class ConformalPredictionWrapper:
    """Calibrate prediction intervals from lower and upper quantile models."""

    def __init__(self, lower_model, upper_model, coverage=0.80):
        if not 0 < coverage < 1:
            raise ValueError("coverage must be between 0 and 1")

        self.lower_model = lower_model
        self.upper_model = upper_model
        self.coverage = coverage
        self.adjustment_ = None

    def calibrate(self, X_cal, y_cal):
        """Estimate the conformal adjustment using calibration data."""
        y_cal = np.asarray(y_cal).ravel()

        lower = np.asarray(self.lower_model.predict(X_cal)).ravel()
        upper = np.asarray(self.upper_model.predict(X_cal)).ravel()

        # CQR conformity scores
        scores = np.maximum(lower - y_cal, y_cal - upper)

        n = len(scores)
        if n == 0:
            raise ValueError("Calibration data must not be empty")

        # Finite-sample conformal quantile
        rank = min(
            int(np.ceil((n + 1) * self.coverage)),
            n,
        )
        self.adjustment_ = float(np.sort(scores)[rank - 1])

        return self

    def predict_interval(self, X):
        """Return calibrated lower and upper prediction bounds."""
        if self.adjustment_ is None:
            raise RuntimeError("Call calibrate() before predict_interval()")

        lower = np.asarray(self.lower_model.predict(X)).ravel()
        upper = np.asarray(self.upper_model.predict(X)).ravel()

        # Ensure quantile predictions are ordered before adjustment
        raw_lower = np.minimum(lower, upper)
        raw_upper = np.maximum(lower, upper)

        calibrated_lower = raw_lower - self.adjustment_
        calibrated_upper = raw_upper + self.adjustment_

        # Trip duration cannot be negative
        calibrated_lower = np.maximum(calibrated_lower, 0)

        return calibrated_lower, calibrated_upper
