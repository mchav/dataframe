# Haskell for Data Analysis

This section ports/mirrors Wes McKinney's book [Python for Data Analysis](https://wesmckinney.com/book/). Examples and organizations are drawn from there. This tutorial assumes an understanding of Haskell.

## Data preparation
Data in the wild doesn't always come in a form that's easy to work with. A data analysis tool should make preparing and cleaning data easy. There are a number of common issues that data analysis too must handle. We'll go through a few common ones and show how to deal with them in Haskell.

### Handling missing data
In Haskell, potentially missing values are represented by a "wrapper" type called [`Maybe`](https://en.wikibooks.org/wiki/Haskell/Understanding_monads/Maybe).

```
ghci> import qualified DataFrame as D
ghci> let df = D.fromColumnList [D.toColumn [Just 1, Just 1, Nothing, Nothing], D.toColumn [Just 6.5, Nothing, Nothing, Just 6.5], D.toColumn [Just 3.0, Nothing, Nothing, Just 3.0]]
ghci> df
---------------------------------------------------
index |       0       |      1       |      2      
------|---------------|--------------|-------------
 Int  | Maybe Integer | Maybe Double | Maybe Double
------|---------------|--------------|-------------
0     | Just 1        | Just 6.5     | Just 3.0    
1     | Just 1        | Nothing      | Nothing     
2     | Nothing       | Nothing      | Nothing     
3     | Nothing       | Just 6.5     | Just 3.0    

```

If we'd like to drop all rows with missing values we can use the `filterJust` function.

```haskell
ghci> D.filterJust "0" df
---------------------------------------------
index |    0    |      1       |      2      
------|---------|--------------|-------------
 Int  | Integer | Maybe Double | Maybe Double
------|---------|--------------|-------------
0     | 1       | Just 6.5     | Just 3.0    
1     | 1       | Nothing      | Nothing     
```

The function filters out the non-`Nothing` values and "unwrap" the `Maybe` type. To filter all `Nothing` values we use the `filterAllJust` function.

```haskell
ghci> D.filterAllJust df
---------------------------------
index |    0    |   1    |   2   
------|---------|--------|-------
 Int  | Integer | Double | Double
------|---------|--------|-------
0     | 1       | 6.5    | 3.0   
```

To fill in the missing values we the impute function which replaces all instances of `Nothing` with a given value.

```haskell
ghci> D.impute "0" (0 :: Integer) df
---------------------------------------------
index |    0    |      1       |      2      
------|---------|--------------|-------------
 Int  | Integer | Maybe Double | Maybe Double
------|---------|--------------|-------------
0     | 1       | Just 6.5     | Just 3.0    
1     | 1       | Nothing      | Nothing     
2     | 0       | Nothing      | Nothing     
3     | 0       | Just 6.5     | Just 3.0    
```

There is no general way to replace ALL nothing values with a default since the default depends on the type. In fact, trying to apply the wrong type to a function throws an error:

```haskell
ghci> D.impute @Double "0" 0 df
*** Exception: 

[Error]: Type Mismatch
        While running your code I tried to get a column of type: "Maybe Double" but column was of type: "Maybe Integer"
        This happened when calling function apply on the column 0



        Try adding a type at the end of the function e.g change
                apply arg1 arg2 to 
                (apply arg1 arg2 :: <Type>)
        or add {-# LANGUAGE TypeApplications #-} to the top of your file then change the call to 
                apply @<Type> arg1 arg2
```

In general, Haskell would usually have a compile-time. But because dataframes are usually run in REPL-like environments which offer immediate feedback to users, `dataframe` is fine turning these into compile-time exceptions.

