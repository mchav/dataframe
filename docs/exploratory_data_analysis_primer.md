# A primer on Exploratory Data Analysis

Exploratory data analysis (EDA), in brief, is what you do when you first get a dataset. EDA should help us answer questions about the data and help us formulate new ones. It is the step before any modelling or inference where we look at the data so we can:

* check for completeness/correctness of data.
* understand the relationships between the explanatory variables.
* understand the relationship between the explanatory and outcome variables.
* preliminarily determine what models would be appropriate for our data. 

It's important for EDA tools to be feature-rich and intuitive so we can answer many different kinds of questions about the data without the tool getting in the way.


There are four types of explanatory data analysis:

* univariate non-graphical analysis
* multivariate non-graphical analysis
* univariate graphical analysis
* multivariate graphical analysis

We will look at each type of EDA and describe how we can use dataframe for each type. We'll be using the [California Housing Dataset](https://www.kaggle.com/datasets/camnugent/california-housing-prices) to demonstrate the concepts as we explain them.

## Univariate non-graphical analysis

Univariate non-graphical analysis should give us a sense of the distribution of our dataset's variables. In the real world our variables are measurable characteristics. How they are distributed (the "sample distribution") and this may often help us estimate the overall distribution ("population distribution") of the variable. For example, if our variable was finishing times for a race, our analysis should be able to answer questions like what was the slowest time, what time did people tend to run, who was the fastest, were all times recorded etc.

For categorical data the best univariate non-graphical analysis is a tabulation of the frequency of each category.

```haskell
ghci> import qualified DataFrame as D
ghci> D.frequencies "ocean_proximity" df

------------------------------------------------------------------------------
index |   Statistic    | <1H OCEAN | INLAND  | ISLAND  | NEAR BAY | NEAR OCEAN
------|----------------|-----------|---------|---------|----------|-----------
 Int  |      Text      |  Integer  | Integer | Integer | Integer  |  Integer  
------|----------------|-----------|---------|---------|----------|-----------
0     | Count          | 9136      | 6551    | 5       | 2290     | 2658      
1     | Percentage (%) | 44        | 31      | 0       | 11       | 12
```

We can also plot similar tables for non-categorical data with a small value set e.g shoe sizes.

For quantitative data our goal is to understand the population distribution through our sample distribution. For a given quantitative variable we typically care about its:

* presence (how much data is missing from each charateristic/variable)
* center (what a "typical" value looks like for some definition of typical),
* spread (how far values are from the "typical" value),
* modality (what are the most popular ranges of values),
* shape (is the data normally distributed? does it skew left or right?),
* and outliers (how common are outliers)

We can calculate sample statistics from the data such as the sample mean, sample variance etc. Although it's most often useful to use graphs to visualize the data's distribution, univariate non-graphical EDA describes aspects of the data's histogram. 

### Missing data
Arguably the first thing to do when presented with a datset is check for null values.

```haskell
ghci> D.columnInfo df
-----------------------------------------------------------------------------
index |    Column Name     | # Non-null Values | # Null Values |     Type    
------|--------------------|-------------------|---------------|-------------
 Int  |       [Char]       |        Int        |      Int      |    [Char]   
------|--------------------|-------------------|---------------|-------------
0     | total_bedrooms     | 20433             | 207           | Maybe Double
1     | ocean_proximity    | 20640             | 0             | Text        
2     | median_house_value | 20640             | 0             | Double      
3     | median_income      | 20640             | 0             | Double      
4     | households         | 20640             | 0             | Double      
5     | population         | 20640             | 0             | Double      
6     | total_rooms        | 20640             | 0             | Double      
7     | housing_median_age | 20640             | 0             | Double      
8     | latitude           | 20640             | 0             | Double      
9     | longitude          | 20640             | 0             | Double
```

It seems we have most of the data except some missing total bedrooms. Dealing with nulls is a separate topic that requires intimate knowledge of the data. So for this initial pass we'll leave out the total_bedrooms variable.

### Central tendency
The central tendency of a distribution describes a "typical" value of that distribution. The most common statistical measures of central tendency are arithmetic mean and median. For symmetric distributions the mean and the median are the same. But for a skewed distribution the mean is pulled towards the "heavier" side wherease the median is more robust to these changes.

For a given column calulating the mean and median is fairly straightfoward and shown below. 
            
```haskell
ghci> D.mean "housing_median_age" df
Just 28.63948643410852
ghci> D.median "housing_median_age" df
Just 29.0
```

Note: the values are displayed with a `Just` to denote that they may not be computable or not exist. Trying to get the mean or median of a non-numeric column would return `Nothing`. `Nothing` is similar to `NULL` in SQL.

### Spread
Spread is a measure of how far away from the center we are still likely to find data values. There are three main measures of spread: variance, mean absolute deviation, standard deviation, and interquartile range.

### Mean absolute deviation
We start by looking at mean absolute deviation since it's the simplest measure of spread. The mean absolute deviation measures how far from the average values are on average. We calcuate it by taking the absolute value of the difference between each observation and the mean of that variable, then finally taking the average of those.

In the housing dataset it'll tell how "typical" our typical home price is.

```haskell
ghci> import Data.Maybe
ghci> m = fromMaybe 0 $ D.mean "median_house_value" df
206855.81690891474
ghci> df |> D.derive "deviation" (D.col "median_house_value" - D.lit m) |> D.select ["median_house_value", "deviation"] |> D.take 10
-----------------------------------------------
index | median_house_value |     deviation     
------|--------------------|-------------------
 Int  |       Double       |       Double      
------|--------------------|-------------------
0     | 452600.0           | 245744.18309108526
1     | 358500.0           | 151644.18309108526
2     | 352100.0           | 145244.18309108526
3     | 341300.0           | 134444.18309108526
4     | 342200.0           | 135344.18309108526
5     | 269700.0           | 62844.18309108526 
6     | 299200.0           | 92344.18309108526 
7     | 241400.0           | 34544.18309108526 
8     | 226700.0           | 19844.18309108526 
9     | 261100.0           | 54244.18309108526
```

Read left to right, we begin by calling `derive` which creates a new column computed from a given expression. The order of arguments is `derive <target column> <expression>  <dataframe>`. We then select only the two columns we want and take the first 10 rows.

This gives us a list of the deviations. From the small sample it does seem like there are some wild deviations. The first one is greater than the mean! How typical is this? Well to answer that we take the average of all these values.

```haskell
ghci> withDeviation = df |> D.derive "deviation" (D.col "median_house_value" - D.lit m) |> "median_house_value" |> D.select ["median_house_value", "deviation"]
ghci> D.mean "deviation" withDeviation
Just 91170.43994367732
```

So the $200'000 deviation we saw in the sample isn't very typical but it raises a question about outliers.
What if we give more weight to the further deviations?


### Standard deviation
That's what standard deviation aims to do. Standard deviation considers the spread of outliers. Instead of calculating the absolute difference of each observation from the mean we calculate the square of the difference. This has the effect of exaggerating further outliers.

```haskell
ghci> sumOfSqureDifferences = fromMaybe 0 $ D.sum "deviation" withDeviation 
ghci> n = fromIntegral $ (fst $ D.dimensions df) - 1
ghci> sqrt (sumOfSqureDifferences / n)
115395.6158744
```
The standard deviation being larger than the mean absolute deviation means we do have some outliers. However, since the difference is fairly small we can conclude that there aren't very many outliers in our dataset.

We can calculate the standard deviation in one line as follows:

```haskell
ghci> D.standardDeviation "median_house_value" df
Just 115395.6158744
```

## Interquartile range (IQR)
A quantile is a value of the distribution such that n% of values in the distribution are smaller than that value. A quartile is a division of the data into four quantiles. So the 1st quantile is a value such that 25% of values are smaller than it. The median is the second quartile. And the third quartile is a value such that 75% of values are smaller than that value. The IQR is the difference between the 3rd and 1st quartiles. It measures how close to middle the middle 50% of values are.

The IQR is a more robust measure of spread than the variance or standard deviation. Any number of values in the top or bottom quarters of the data can be moved any distance from the median without affecting the IQR at all. More practically, a few extreme outliers have little or no effect on the IQR

For our dataset:

```haskell
ghci> D.interQuartileRange "median_house_value" df
Just 145158.3333333336
```

This is larger than the standard deviation but not by much. This means that outliers don't have a significant influence on the distribution and most values are close to typical.

### Variance
Variance is the square of the standard deviation. It is much more sensitive to outliers. Variance does not have the same units as our original variable (it is in units squared). Therefore, it's much more difficult to interpret.

In our example it's a very large number:

``` haskell
ghci> D.variance  "median_house_value" df
Just 1.3315503000818077e10
```

The variance is more useful when comparing different datasets. If the variance of house prices in Minnesota was lower than California this would mean there were much fewer really cheap and really expensive house in Minnesota.

## Shape
Skewness measures how left or right shifted a distribution is from a normal distribution. A positive skewness means the distribution is left shifted, a negative skew means the distribution is right shifted.

The formula for skewness is the mean cubic deviation divided by the cube of the standard deviation. It captures the relationship between the mean deviation (asymmetry of the data) and the standard deviation (spread of the data).

The intuition behind why a positive skew is left shifted follows from the formula. The numerator is more sensitive to outliers. So the futher left a distribution is the more the right-tail values will be exaggerated by the cube causing the skewness to be positive.

A skewness score between -0.5 and 0.5 means the data has little skew. A score between -0.5 and -1 or 0.5 and 1 means the data has moderate skew. A skewness greater than 1 or less than -1 means the data is heavily skewed.

```haskell
ghci> D.skewness "median_house_value" df
Just 0.9776922140978703
```
So the median house value is moderately skewed to the left. That is, there are more houses that are cheaper than the mean values and a tail of expensive outliers. Having lived in California, I can confirm that this data reflects reality.


## Summarising the data

We can get all these statistics with a single command:

```haskell
ghci> D.summarize df
------------------------------------------------------------------------------------------------------------------------------------------
index | Statistic | median_house_value | median_income | households | population | total_rooms | housing_median_age | latitude | longitude
------|-----------|--------------------|---------------|------------|------------|-------------|--------------------|----------|----------
 Int  |   Text    |       Double       |    Double     |   Double   |   Double   |   Double    |       Double       |  Double  |  Double  
------|-----------|--------------------|---------------|------------|------------|-------------|--------------------|----------|----------
0     | Mean      | 206855.82          | 3.87          | 499.54     | 1425.48    | 2635.76     | 28.64              | 35.63    | -119.57  
1     | Minimum   | 14999.0            | 0.5           | 1.0        | 3.0        | 2.0         | 1.0                | 32.54    | -124.35  
2     | 25%       | 119600.0           | 2.56          | 280.0      | 787.0      | 1447.42     | 18.0               | 33.93    | -121.8   
3     | Median    | 179700.0           | 3.53          | 409.0      | 1166.0     | 2127.0      | 29.0               | 34.26    | -118.49  
4     | 75%       | 264758.33          | 4.74          | 605.0      | 1725.0     | 3148.0      | 37.0               | 37.71    | -118.01  
5     | Max       | 500001.0           | 15.0          | 6082.0     | 35682.0    | 39320.0     | 52.0               | 41.95    | -114.31  
6     | StdDev    | 115395.62          | 1.9           | 382.33     | 1132.46    | 2181.62     | 12.59              | 2.14     | 2.0      
7     | IQR       | 145158.33          | 2.18          | 325.0      | 938.0      | 1700.58     | 19.0               | 3.78     | 3.79     
8     | Skewness  | 0.98               | 1.65          | 3.41       | 4.94       | 4.15        | 6.0e-2             | 0.47     | -0.3
```

As a recap we'll go over what this tells us about the data:
* median_house_value: house prices tend to be close to the median but there are some pretty expensive houses.
* median_income: incomes are also generally fairly typical (small standard deviation with median close to mean) but there are some really rich people (high skewness).
* households: household sizes are very similar across the sample and they tend to be smaller.
* population: California is generally very sparsely populated (low skewness) with some REALLY densely populated areas (high max/ low IQR).
* total_rooms: a lot of the blocks have few rooms (Again sparse population) but there are some very dense areas (high max).
* housing_median_age: there are as many new houses as there are old (skewness close to 0) and not many extremes (low max, standard deviation lower than IQR)
* latitude: the south has slightly more people than the north (moderate skew)
* longitude: most houses are in the west coast (moderate right skew)

