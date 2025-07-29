dataframe
=========

A fast, safe, and intuitive DataFrame library.

*******************************
Why use this DataFrame library?
*******************************

* Encourages concise, declarative, and composable data pipelines.
* Static typing makes code easier to reason about and catches many bugs at compile time—before your code ever runs.
* Delivers high performance thanks to Haskell’s optimizing compiler and efficient memory model.
* Designed for interactivity: expressive syntax, helpful error messages, and sensible defaults.
* Works seamlessly in both command-line and notebook environments—great for exploration and scripting alike.

*************
Example usage
*************
Looking through the structure of the columns.

.. code-block:: haskell    

    ghci> import qualified DataFrame as D
    ghci> import DataFrame ((|>))
    ghci> df <- D.readCsv "./data/housing.csv"
    ghci> D.describeColumns df
    --------------------------------------------------------------------------------------------------------------------
    index |    Column Name     | # Non-null Values | # Null Values | # Partially parsed | # Unique Values |     Type    
    ------|--------------------|-------------------|---------------|--------------------|-----------------|-------------
     Int  |        Text        |        Int        |      Int      |        Int         |       Int       |     Text    
    ------|--------------------|-------------------|---------------|--------------------|-----------------|-------------
    0     | total_bedrooms     | 20433             | 207           | 0                  | 1924            | Maybe Double
    1     | ocean_proximity    | 20640             | 0             | 0                  | 5               | Text        
    2     | median_house_value | 20640             | 0             | 0                  | 3842            | Double      
    3     | median_income      | 20640             | 0             | 0                  | 12928           | Double      
    4     | households         | 20640             | 0             | 0                  | 1815            | Double      
    5     | population         | 20640             | 0             | 0                  | 3888            | Double      
    6     | total_rooms        | 20640             | 0             | 0                  | 5926            | Double      
    7     | housing_median_age | 20640             | 0             | 0                  | 52              | Double      
    8     | latitude           | 20640             | 0             | 0                  | 862             | Double      
    9     | longitude          | 20640             | 0             | 0                  | 844             | Double

Automatically generate column names.

.. code-block:: haskell
    ghci> import DataFrame.Functions (declareColumns)
    ghci> :exposeColumns df

We can use the generated columns in expressions.

.. code-block:: haskell
    ghci> df |> D.groupBy ["ocean_proximity"] |> D.aggregate [(F.mean median_house_value) `F.as` "avg_house_value" ]
    --------------------------------------------
    index | ocean_proximity |  avg_house_value  
    ------|-----------------|-------------------
     Int  |      Text       |       Double      
    ------|-----------------|-------------------
    0     | <1H OCEAN       | 240084.28546409807
    1     | INLAND          | 124805.39200122119
    2     | ISLAND          | 380440.0          
    3     | NEAR BAY        | 259212.31179039303
    4     | NEAR OCEAN      | 249433.97742663656


Create a new column based on other columns.

.. code-block:: haskell
    ghci> df |> D.derive "rooms_per_household" (total_rooms / households) |> D.take 10
    --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    index | longitude | latitude | housing_median_age | total_rooms | total_bedrooms | population | households |   median_income    | median_house_value | ocean_proximity | rooms_per_household
    ------|-----------|----------|--------------------|-------------|----------------|------------|------------|--------------------|--------------------|-----------------|--------------------
     Int  |  Double   |  Double  |       Double       |   Double    |  Maybe Double  |   Double   |   Double   |       Double       |       Double       |      Text       |       Double       
    ------|-----------|----------|--------------------|-------------|----------------|------------|------------|--------------------|--------------------|-----------------|--------------------
    0     | -122.23   | 37.88    | 41.0               | 880.0       | Just 129.0     | 322.0      | 126.0      | 8.3252             | 452600.0           | NEAR BAY        | 6.984126984126984  
    1     | -122.22   | 37.86    | 21.0               | 7099.0      | Just 1106.0    | 2401.0     | 1138.0     | 8.3014             | 358500.0           | NEAR BAY        | 6.238137082601054  
    2     | -122.24   | 37.85    | 52.0               | 1467.0      | Just 190.0     | 496.0      | 177.0      | 7.2574             | 352100.0           | NEAR BAY        | 8.288135593220339  
    3     | -122.25   | 37.85    | 52.0               | 1274.0      | Just 235.0     | 558.0      | 219.0      | 5.6431000000000004 | 341300.0           | NEAR BAY        | 5.8173515981735155 
    4     | -122.25   | 37.85    | 52.0               | 1627.0      | Just 280.0     | 565.0      | 259.0      | 3.8462             | 342200.0           | NEAR BAY        | 6.281853281853282  
    5     | -122.25   | 37.85    | 52.0               | 919.0       | Just 213.0     | 413.0      | 193.0      | 4.0368             | 269700.0           | NEAR BAY        | 4.761658031088083  
    6     | -122.25   | 37.84    | 52.0               | 2535.0      | Just 489.0     | 1094.0     | 514.0      | 3.6591             | 299200.0           | NEAR BAY        | 4.9319066147859925 
    7     | -122.25   | 37.84    | 52.0               | 3104.0      | Just 687.0     | 1157.0     | 647.0      | 3.12               | 241400.0           | NEAR BAY        | 4.797527047913447  
    8     | -122.26   | 37.84    | 42.0               | 2555.0      | Just 665.0     | 1206.0     | 595.0      | 2.0804             | 226700.0           | NEAR BAY        | 4.294117647058823  
    9     | -122.25   | 37.84    | 52.0               | 3549.0      | Just 707.0     | 1551.0     | 714.0      | 3.6912000000000003 | 261100.0           | NEAR BAY        | 4.970588235294118

If two columns don't type check we catch this with a type error instead of a runtime error.

.. code-block:: haskell

    ghci> df |> D.derive "nonsense_feature" (latitude + ocean_proximity) |> D.take 10
    
    <interactive>:14:47: error: [GHC-83865]
        • Couldn't match type ‘Text’ with ‘Double’
          Expected: Expr Double
            Actual: Expr Text
        • In the second argument of ‘(+)’, namely ‘ocean_proximity’
          In the second argument of ‘derive’, namely
            ‘(latitude + ocean_proximity)’
          In the second argument of ‘(|>)’, namely
            ‘derive "nonsense_feature" (latitude + ocean_proximity)’

Key features in example:

* Intuitive, SQL-like API to get from data to insights.
* Create type-safe references to columns in a dataframe using :exponseColumns
* Type-safe column transformations for faster and safer exploration.
* Fluid, chaining API that makes code easy to reason about.


.. toctree::
   :maxdepth: 2

   haskell_for_data_analysis
   exploratory_data_analysis_primer
   coming_from_pandas
   coming_from_polars
