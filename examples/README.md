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
--------------------------------------------------
index | median_house_value | predicted_house_value
------|--------------------|----------------------
 Int  |       Double       |         Float        
------|--------------------|----------------------
0     | 452600.0           | 414079.94            
1     | 358500.0           | 423011.94            
2     | 352100.0           | 383239.06            
3     | 341300.0           | 324928.94            
4     | 342200.0           | 256934.23            
5     | 269700.0           | 264944.84            
6     | 299200.0           | 259094.13            
7     | 241400.0           | 257224.55            
8     | 226700.0           | 201753.69            
9     | 261100.0           | 268698.7
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
Iteration :100 | Training Set Loss: 0.49717537 | Test Set Loss: 0.5145686
Iteration :200 | Training Set Loss: 0.40175727 | Test Set Loss: 0.4315569
Iteration :300 | Training Set Loss: 0.33975253 | Test Set Loss: 0.373216
Iteration :400 | Training Set Loss: 0.3006259 | Test Set Loss: 0.33355427
Iteration :500 | Training Set Loss: 0.27235344 | Test Set Loss: 0.30395
Iteration :600 | Training Set Loss: 0.2493768 | Test Set Loss: 0.2796418
Iteration :700 | Training Set Loss: 0.22940455 | Test Set Loss: 0.2585228
Iteration :800 | Training Set Loss: 0.21151513 | Test Set Loss: 0.23950206
Iteration :900 | Training Set Loss: 0.19527085 | Test Set Loss: 0.22228393
Iteration :1000 | Training Set Loss: 0.18052626 | Test Set Loss: 0.20675912
Iteration :1100 | Training Set Loss: 0.16718781 | Test Set Loss: 0.1927444
Iteration :1200 | Training Set Loss: 0.15518206 | Test Set Loss: 0.1801689
Iteration :1300 | Training Set Loss: 0.14442518 | Test Set Loss: 0.16895148
Iteration :1400 | Training Set Loss: 0.13482077 | Test Set Loss: 0.15899345
Iteration :1500 | Training Set Loss: 0.12626256 | Test Set Loss: 0.1501799
Iteration :1600 | Training Set Loss: 0.118641265 | Test Set Loss: 0.14239211
Iteration :1700 | Training Set Loss: 0.11185091 | Test Set Loss: 0.13551587
Iteration :1800 | Training Set Loss: 0.10579081 | Test Set Loss: 0.12944478
Iteration :1900 | Training Set Loss: 0.10037044 | Test Set Loss: 0.12407951
Iteration :2000 | Training Set Loss: 9.5508784e-2 | Test Set Loss: 0.11933816
Iteration :2100 | Training Set Loss: 9.113287e-2 | Test Set Loss: 0.11514034
Iteration :2200 | Training Set Loss: 8.7180905e-2 | Test Set Loss: 0.11141248
Iteration :2300 | Training Set Loss: 8.3601296e-2 | Test Set Loss: 0.1080656
Iteration :2400 | Training Set Loss: 8.034743e-2 | Test Set Loss: 0.10508868
...
Test Set Predictions are are as follows: 
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Setosa, Predicted label: Setosa
Actual label: Versicolor, Predicted label: Versicolor
Actual label: Versicolor, Predicted label: Versicolor
Actual label: Versicolor, Predicted label: Versicolor
Actual label: Versicolor, Predicted label: Versicolor
Actual label: Versicolor, Predicted label: Versicolor
Actual label: Versicolor, Predicted label: Versicolor
Actual label: Versicolor, Predicted label: Virginica
Actual label: Versicolor, Predicted label: Versicolor
Actual label: Versicolor, Predicted label: Versicolor
```