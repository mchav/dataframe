# Running the examples

## California housing

Preparation:
This has a hasktorch integration that requires dynamic linking to be enabled.

`./setup_torch` does just that. It creates a cabal.project.local file that has dunamic linking enabled.

This example also requires GHC <= 9.8.

Running:
`cabal run california_housing`.

Expected output:

```
Training linear regression model...
Iteration: 10000 | Loss: Tensor Float []  5.0225e9   
Iteration: 20000 | Loss: Tensor Float []  4.9093e9   
Iteration: 30000 | Loss: Tensor Float []  4.8576e9   
Iteration: 40000 | Loss: Tensor Float []  4.8333e9   
Iteration: 50000 | Loss: Tensor Float []  4.8217e9   
Iteration: 60000 | Loss: Tensor Float []  4.8160e9   
Iteration: 70000 | Loss: Tensor Float []  4.8130e9   
Iteration: 80000 | Loss: Tensor Float []  4.8114e9   
Iteration: 90000 | Loss: Tensor Float []  4.8105e9   
Iteration: 100000 | Loss: Tensor Float []  4.8099e9   
-------------------------------------------
 median_house_value | predicted_house_value
--------------------|----------------------
       Double       |         Float        
--------------------|----------------------
 452600.0           | 414079.94            
 358500.0           | 423011.94            
 352100.0           | 383239.06            
 341300.0           | 324928.94            
 342200.0           | 256934.23            
 269700.0           | 264944.84            
 299200.0           | 259094.13            
 241400.0           | 257224.55            
 226700.0           | 201753.69            
 261100.0           | 268698.7
```

## Chipotle

Running:
`cabal run chipotle`.

## One billion row challenge

Running:
`cabal run one_billion_row_challenge`.

## Iris classification using Torch

Preparation:
This has a hasktorch integration that requires dynamic linking to be enabled.

`./setup_torch` does just that. It creates a cabal.project.local file that has dunamic linking enabled.

This example also requires GHC <= 9.8.

Running:
`cabal run iris`.

Expected output:

```
.....................................
.....................................
Training Set Summary is as follows:
====== Confusion Matrix ========
          0     1     2
    0 36.00  0.00  0.00
    1  0.00 31.00  0.00
    2  0.00  1.00 36.00

=========== Classwise Metrics =============
---------------------------------
  variety   | precision | recall
------------|-----------|--------
    Iris    |   Float   |  Float
------------|-----------|--------
 Setosa     | 1.0       | 1.0
 Versicolor | 1.0       | 0.96875
 Virginica  | 0.972973  | 1.0

.....................................
.....................................
Test Set Summary is as follows:
====== Confusion Matrix ========
          0     1     2
    0 14.00  0.00  0.00
    1  0.00 16.00  0.00
    2  0.00  2.00 14.00

=========== Classwise Metrics =============
-----------------------------------
  variety   | precision |  recall
------------|-----------|----------
    Iris    |   Float   |   Float
------------|-----------|----------
 Setosa     | 1.0       | 1.0
 Versicolor | 1.0       | 0.8888889
 Virginica  | 0.875     | 1.0
```