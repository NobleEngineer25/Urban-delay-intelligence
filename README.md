## Baseline Model Evaluation

The initial baseline model predicts trip duration using historical average trip durations grouped by pickup community area, dropoff community area, and hour of day.

- **Evaluation strategy:** Chronological 80/20 train-test split
- **Fallback strategy:** Mean trip duration from the training dataset
- **Evaluation metric:** Mean Absolute Error (MAE)
- **Test MAE:** 6.60 minutes
- **Experiment tracking:** MLflow with SQLite backend

The baseline provides a reference point for evaluating machine learning models. Historical averages are calculated using training data only to avoid data leakage.

## Random Forest Model Evaluation

A Random Forest regression model was trained using temporal and spatial features to predict trip duration.

- **Features:** Pickup community area, dropoff community area, hour, day of week, weekend indicator, month, and rush-hour indicator
- **Evaluation strategy:** Chronological 80/20 train-test split
- **Model configuration:** 60 trees, maximum depth of 12
- **Test MAE:** 6.88 minutes

The Random Forest model did not outperform the historical baseline in the current experiment. This provides a useful reference for evaluating alternative algorithms and feature-engineering strategies.

## LightGBM Quantile Regression

LightGBM quantile regression was implemented to predict trip duration at multiple quantiles and estimate uncertainty around travel-time predictions.

Three separate models were trained:

- **10th percentile (0.1):** Lower travel-time estimate
- **50th percentile (0.5):** Median travel-time estimate
- **90th percentile (0.9):** Upper travel-time estimate

### Configuration

- **Input features:** Pickup community area, dropoff community area, hour, day of week, weekend indicator, month, and rush-hour indicator
- **Target:** `trip_duration_minutes`
- **Evaluation strategy:** Chronological 80/20 train-test split
- **Training examples:** 257,468
- **Test examples:** 64,368
- **Number of estimators:** 300
- **Learning rate:** 0.05
- **Number of leaves:** 31
- **Experiment tracking:** MLflow

### Evaluation Results

| Metric                            |        Result |
| --------------------------------- | ------------: |
| Median prediction MAE             |  5.67 minutes |
| 10th-percentile quantile loss     |          0.93 |
| 50th-percentile quantile loss     |          2.83 |
| 90th-percentile quantile loss     |          2.25 |
| 80% prediction interval coverage  |        78.91% |
| Average prediction interval width | 14.89 minutes |

The median LightGBM model achieved a lower MAE than the historical baseline and Random Forest in the current experiments. The prediction interval between the 10th and 90th percentiles covered 78.91% of observed test outcomes, compared with a nominal target of 80%.

Negative lower-bound predictions are clipped to zero to maintain physically meaningful travel-time estimates. This adjustment applies to the prediction interval, not the median model's MAE calculation.

The interval coverage is close to the nominal target but has not been calibrated to guarantee 80% coverage. Further evaluation is needed to assess calibration across different times, locations, and trip durations.

## Model Comparison

| Model                                 |     Test MAE |
| ------------------------------------- | -----------: |
| Historical baseline                   | 6.60 minutes |
| Random Forest                         | 6.88 minutes |
| LightGBM quantile regression (median) | 5.67 minutes |

LightGBM achieved the lowest reported MAE among the evaluated models. These results are preliminary until the test records and evaluation filters are confirmed to be identical across all experiments.
