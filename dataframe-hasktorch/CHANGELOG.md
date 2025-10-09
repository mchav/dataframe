# Revision history for dataframe-hasktorch

## 0.1.0.2

* Add unbox constraint to `flattenFeatures`.

## 0.1.0.1

* Export `toIntTensor` function that converts a dataframe to an Int tensor.
* `toTensor` now does automatic type conversion. `Nothing` is turned into `NaN` and other numeric types are changed to `Float` (be careful of precision errors).

## 0.1.0.0

* Export `toTensor` function that converts a dataframe to a tensor.
