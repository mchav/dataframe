# Haskell for Data Analysis

This section ports/mirrors part of Wes McKinney's book [Python for Data Analysis](https://wesmckinney.com/book/). Examples and organizations are drawn from there. This tutorial does not assume an understanding of Haskell.

## What is a dataframe?

A DataFrame is like a spreadsheet or a table — it organizes data into rows and columns.

* Each column has a name (like "Name", "Age", or "Price") and usually contains the same type of information (like numbers or text).

* Each row is one entry or record — like a person, a product, or a day’s worth of sales.

Imagine an Excel sheet or Google Sheets file:

| Name	| Age	| City      |
|-------|-------|-----------|
| Alice	| 30	| New York  |
| Bob	| 25	| San Diego |
| Cara	| 35	| Austin    |

That’s essentially a DataFrame!

DataFrames make it easy to:
* Look at your data
* Filter or sort it (like showing only people over 30)
* Do math on it (like averaging ages)
* Clean it (like removing bad or incomplete data)

They're a key tool for data scientists, analysts, and programmers when working with data. This guide is about how to use dataframes in a language called Haskell.

## Why use Haskell?

* Having types around eliminates many kinds of bugs before you even run the code.
* It's easy to write pipelines.
* The Haskell compiler has a lot of optimization that makes code very fast.
* The syntax is more approachable than other compiled languages' dataframes.

## Installing the tooling
Check the [README](https://github.com/mchav/dataframe?tab=readme-ov-file#installing) for how to install the required tooling.


## Getting the data
Data enters a computer program in one of two ways:
* manual entry of the data by a human, or,
* through a file whose data was the output of another computer program or the result of manual entry.

We will show how to do both in dataframes.

### Entering the data manually

I live in Seattle where the weather is a legitimate, non-small-talk topic of conversation for most of the year. At any given point in time I care about what the weather is and what it will be. I'd like to do some simple computation on a week's worth of high and low temperatures. A week of data is small enough that I can enter it myself so I'll do just that.

```haskell
ghci> let df = D.fromNamedColumns [("Day", D.fromList ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]), ("High Temperature (Celcius)", D.fromList [24, 20, 22, 23, 25, 26, 26]), ("Low Temperature (Celcius)", D.fromList [14, 13, 13, 13, 14, 15, 15])]
ghci> df
--------------------------------------------------------------------------
index |    Day    | High Temperature (Celcius) | Low Temperature (Celcius)
------|-----------|----------------------------|--------------------------
 Int  |  [Char]   |          Integer           |          Integer         
------|-----------|----------------------------|--------------------------
0     | Monday    | 24                         | 14                       
1     | Tuesday   | 20                         | 13                       
2     | Wednesday | 22                         | 13                       
3     | Thursday  | 23                         | 13                       
4     | Friday    | 25                         | 14                       
5     | Saturday  | 26                         | 15                       
6     | Sunday    | 26                         | 15
```

We use the function `fromNamedColumns` to create a dataframe from manually entered data. The format of the function is `fromNamedColumns [(<name>, <column>), (<name>, <column>),...]`. It has an equivalent for data without column names called `fromUnnamedColumns`.

```haskell
ghci> let df = D.fromUnnamedColumns [D.fromList ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], D.fromList [24, 20, 22, 23, 25, 26, 26], D.fromList [14, 13, 13, 13, 14, 15, 15]]
ghci> df
-------------------------------------
index |     0     |    1    |    2   
------|-----------|---------|--------
 Int  |  [Char]   | Integer | Integer
------|-----------|---------|--------
0     | Monday    | 24      | 14     
1     | Tuesday   | 20      | 13     
2     | Wednesday | 22      | 13     
3     | Thursday  | 23      | 13     
4     | Friday    | 25      | 14     
5     | Saturday  | 26      | 15     
6     | Sunday    | 26      | 15
```

This function automatically names columns with numbers 0 to n. This is generally bad practice (everything must have a descriptive name) but is useful for an initial entry where the columns are unknown/have no name.

#### Aside: calling functions in Haskell
Functions in Haskell are slightly different from functions in other languages. In most other languages, parenthese sare used to signal function calls. E.g. in Python calling the `print` function looks like `print("Hello World")`. Similarly function with two arguments has both arguments go in the parentheses: `max(1, 5)`. In Haskell, functions are called without parentheses: `print 5` or `max 1 5`. Parentheses are used in two situations:
* To determine precedence (differentiating between `(not True) && False` vs `not (True && False)`)
* To define a tuple. This is how they are used in the `fromNamedColumns` function. The argument in that function is a list of tuples.

#### Aside: understanding error messages
Programming is as much about how things can fail as much as it is about how they work. A lot more of your time will likely be spent understanding addressing errors in your code than will be spent coding. Hopefully, the Haskell compiler will be a good companion through this journey.

Since Haskell prides itself in catching errors at compile time, it helps to understand the compiler's errors. These may change from release to release but the intuition of how to read these should be useful regardless.

Not a lot of programming guides do this but I'd like to focus on what error messages you will typically see when small things go wrong. We'll keep this as an on going series as we go.

##### Typo in the function name

```haskell
ghci> let df = D.fromUnamedColumns [D.fromList ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], D.fromList [24, 20, 22, 23, 25, 26, 26], D.fromList [14, 13, 13, 13, 14, 15, 15]]

<interactive>:1:10: error: [GHC-76037]
    Not in scope: ‘D.fromUnamedColumns’
    Suggested fix:
      Perhaps use one of these:
        ‘D.fromUnnamedColumns’ (imported from DataFrame),
        ‘D.fromNamedColumns’ (imported from DataFrame)
```

This is a subtle error. The above example is missing an extra `n` in `Unnamed`. What the error means is: given everything I have access to (the so-called "scope") there is nothing by the name of `D.fromUnamedColumns`. Haskell then suggests some names that look close to the name you typed. This is the easiest kind of error to read.

##### Forgetting `D.fromList`
`[1,2,3]` is a regular Haskell list. Our function `D.fromList` takes the list and converts it to a dataframe column. Forgetting to add the `fromList` results in a type error:

```haskell
ghci> let df = D.fromUnnamedColumns [["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], D.fromList [24, 20, 22, 23, 25, 26, 26], D.fromList [14, 13, 13, 13, 14, 15, 15]]

<interactive>:4:32: error: [GHC-83865]
    • Couldn't match expected type ‘D.Column’ with actual type ‘[a0]’
    • In the expression:
        ["Monday", "Tuesday", "Wednesday", "Thursday", ....]
      In the first argument of ‘D.fromUnnamedColumns’, namely
        ‘[["Monday", "Tuesday", "Wednesday", ....],
          D.fromList [24, 20, ....], D.fromList [14, 13, ....]]’
      In the expression:
        D.fromUnnamedColumns
          [["Monday", "Tuesday", "Wednesday", ....],
           D.fromList [24, 20, ....], D.fromList [14, 13, ....]]
```

This is generally what the error messages will look like when you pass the wrong "kind" of thing into a function. E.g. a number into a function that expects words.

We'll look more at types later but what this message says is: I expected a Column but you gave me a list with some `a0`'s (a0 is a placeholder for "something"). It then goes on to tell you where exactly the error occured. Each subsequent line "zooms out" where the problem is. So the order is: input with error -> function with error -> expression with error.

We'll see more of these as we go and we'll unpack how to check them later.

##### Forgetting quotation marks around Strings

```haskell
ghci> let df = D.fromUnnamedColumns [D.fromList ["Monday, "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], D.fromList [24, 20, 22, 23, 25, 26, 26], D.fromList [14, 13, 13, 13, 14, 15, 15]]

<interactive>:8:203: error: [GHC-21231]
    lexical error in string/character literal at end of input
```

This error is more opaque. It tells you that by the time it got to the end of reading your code something didn't balance. When you see this error, you should make sure to check for unclosed parentheses or quotes.

### Getting the data from a file

Most times you encouter data it is in a file (or spread amongst many files). Naturally, a tool for analysing data must support fast and easy file processing. Comma-separated-value (CSV) files are easily the most popular format for tabular data. Reading them is as simple as calling the function `readCsv`.

```haskell
ghci> df <- D.readCsv "./data/housing.csv" 
ghci> D.take 10 df
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
index | longitude | latitude | housing_median_age | total_rooms | total_bedrooms | population | households |   median_income    | median_house_value | ocean_proximity
------|-----------|----------|--------------------|-------------|----------------|------------|------------|--------------------|--------------------|----------------
 Int  |  Double   |  Double  |       Double       |   Double    |  Maybe Double  |   Double   |   Double   |       Double       |       Double       |      Text      
------|-----------|----------|--------------------|-------------|----------------|------------|------------|--------------------|--------------------|----------------
0     | -122.23   | 37.88    | 41.0               | 880.0       | Just 129.0     | 322.0      | 126.0      | 8.3252             | 452600.0           | NEAR BAY       
1     | -122.22   | 37.86    | 21.0               | 7099.0      | Just 1106.0    | 2401.0     | 1138.0     | 8.3014             | 358500.0           | NEAR BAY       
2     | -122.24   | 37.85    | 52.0               | 1467.0      | Just 190.0     | 496.0      | 177.0      | 7.2574             | 352100.0           | NEAR BAY       
3     | -122.25   | 37.85    | 52.0               | 1274.0      | Just 235.0     | 558.0      | 219.0      | 5.6431000000000004 | 341300.0           | NEAR BAY       
4     | -122.25   | 37.85    | 52.0               | 1627.0      | Just 280.0     | 565.0      | 259.0      | 3.8462             | 342200.0           | NEAR BAY       
5     | -122.25   | 37.85    | 52.0               | 919.0       | Just 213.0     | 413.0      | 193.0      | 4.0368             | 269700.0           | NEAR BAY       
6     | -122.25   | 37.84    | 52.0               | 2535.0      | Just 489.0     | 1094.0     | 514.0      | 3.6591             | 299200.0           | NEAR BAY       
7     | -122.25   | 37.84    | 52.0               | 3104.0      | Just 687.0     | 1157.0     | 647.0      | 3.12               | 241400.0           | NEAR BAY       
8     | -122.26   | 37.84    | 42.0               | 2555.0      | Just 665.0     | 1206.0     | 595.0      | 2.0804             | 226700.0           | NEAR BAY       
9     | -122.25   | 37.84    | 52.0               | 3549.0      | Just 707.0     | 1551.0     | 714.0      | 3.6912000000000003 | 261100.0           | NEAR BAY
```

We've introduced a new function in the example above. The `take` function, given a number `n` and a dataframe, cuts everything but the first `n` rows of a dataframe. We use the function so we can check a few rows of a dataframe.

#### Aside: checking the type of a function
When calling a function you always need to know:
* What it takes in
* The order in which it takes its inputs
* What the function spits out.

In your ghci session, you can inspect all the above using the `:t` macro.

```haskell
ghci> :t df
df :: DataFrame
ghci> :t D.take
D.take :: Int -> DataFrame -> DataFrame
```

As expected, `df` is a `DataFrame`. `take` is a function that accepts an integer, and a dataframe, and gives you a dataframe.

The above lets us inspect the type of a function. You'll hear this word a lot in the Haskell world. Types are pretty important. They are an agreement between you and the function that determine how you get the "right" result from a function. Getting the types wrong means breaking that agreement. Some languages let you break these promises (that's if they even require them) and tell you only when the damage is done. Haskell asks you to be clearer about the promises you make and constantly checks that you uphold your end of the agreement.

What we call a promise (for example, `D.take :: Int -> DataFrame -> DataFrame`) is more typically called a function signature. So let's use that term more explicity.

The way you read Haskell function signatures is with the last arrow shows you what the result type is. The stuff before the last arrow is all the arguments to the function in the order they come. Typically languages have a more visible separation between argument types and return types.

For example in Java functions look like:

```java
public String greetNTimes(int n, String name) { ... }
```

In Haskell, the same function would look like:

```haskell
greetNTimes :: Int -> String -> String
greetNTimes n name = ...
```

Why the arrows vs the parentheses? Functions in Haskell can be partially applied so you don't need to always specify all arguments. For example, if you wanted to make a `greet2Times` in Java it would be:

```java
public String greet2Times(String name) {
        return greetNTimes(2, name);
}
```

We need to explicitly bubble the argument down to the `greetNTimes` function. In Haskell, the same function could be written as:

```haskell
greet2Times :: String -> String
greet2Times = greetNTimes 2
```

[_Learn more about partial application_](https://wiki.haskell.org/index.php?title=Partial_application)

We'll make it a habit to always inspect a function's structure whenever we introduce one.

## Peeking into the data

So, you have some data in a dataframe. What do you do with it now? Typically, you'd try and understand the structure of the data: what are the names of the different columns, what kind of data is in each column, how many values are in the data etc. We provide three functions to help do this:

* `take`,
* `describeColumns`, and,
* `summarize`

We've already covered `take` in passing. The function takes a given number of rows from a dataframe.

`describeColumns` tells you what type of data is in each column, the number of unique values, the number of null rows and the rows where we couldn't automatically figure out the type.

```haskell
ghci> :t D.describeColumns
D.describeColumns :: DataFrame -> DataFrame
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
```

The second function tells us more about the distribution of our data.

```haskell
ghci> :t D.summarize
D.summarize :: DataFrame -> DataFrame
ghci> D.summarize df
------------------------------------------------------------------------------------------------------------------------------------------
index | Statistic | longitude | latitude | housing_median_age | total_rooms | population | households | median_income | median_house_value
------|-----------|-----------|----------|--------------------|-------------|------------|------------|---------------|-------------------
 Int  |   Text    |  Double   |  Double  |       Double       |   Double    |   Double   |   Double   |    Double     |       Double      
------|-----------|-----------|----------|--------------------|-------------|------------|------------|---------------|-------------------
0     | Mean      | -119.57   | 35.63    | 28.64              | 2635.76     | 1425.48    | 499.54     | 3.87          | 206855.82         
1     | Minimum   | -124.35   | 32.54    | 1.0                | 2.0         | 3.0        | 1.0        | 0.5           | 14999.0           
2     | 25%       | -121.8    | 33.93    | 18.0               | 1447.42     | 787.0      | 280.0      | 2.56          | 119600.0          
3     | Median    | -118.49   | 34.26    | 29.0               | 2127.0      | 1166.0     | 409.0      | 3.53          | 179700.0          
4     | 75%       | -118.01   | 37.71    | 37.0               | 3148.0      | 1725.0     | 605.0      | 4.74          | 264758.33         
5     | Max       | -114.31   | 41.95    | 52.0               | 39320.0     | 35682.0    | 6082.0     | 15.0          | 500001.0          
6     | StdDev    | 2.0       | 2.14     | 12.59              | 2181.56     | 1132.43    | 382.32     | 1.9           | 115392.82         
7     | IQR       | 3.79      | 3.78     | 19.0               | 1700.58     | 938.0      | 325.0      | 2.18          | 145158.33         
8     | Skewness  | -0.3      | 0.47     | 6.0e-2             | 4.15        | 4.94       | 3.41       | 1.65          | 0.98
```

Knowing the distribution of your data from a glimpse helps you get an initial read of what the data looks like. Coupled with plotting, such techniques are a vital part of exploratory data analysis.

#### Aside: a look at type errors
Type errors, as we said before, are probably the most common type of error you'll encounter when working with code in Haskell. When understood and used well they are a valuable tool for understanding the structure of your code. Errors, in this case, are not something to avoid or to be scared of. They are a conversation with the compiler about your code.

Since Haskell's vocabulary for understanding your code is types some errors that don't even seem like type errors are interpreted as such. For example, suppose we forget to put a comma in the `fromUnnamedColumns` example.

```haskell
ghci> let df = D.fromUnnamedColumns [D.fromList ["Monday" "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], D.fromList [24, 20, 22, 23, 25, 26, 26], D.fromList [14, 13, 13, 13, 14, 15, 15]]

<interactive>:13:44: error: [GHC-39999]
    • No instance for ‘Data.String.IsString (Text -> Text)’
        arising from the literal ‘"Monday"’
        (maybe you haven't applied a function to enough arguments?)
    • In the expression: "Monday" "Tuesday"
      In the first argument of ‘D.fromList’, namely
        ‘["Monday" "Tuesday", "Wednesday", "Thursday", "Friday", ....]’
      In the expression:
        D.fromList
          ["Monday" "Tuesday", "Wednesday", "Thursday", "Friday", ....]
```

Here, we removed the comma between `"Monday"` and `"Tuesday"`. You would assume that the error message would be `missing ',' between "Monday" and "Tuesday"`. Instead, Haskell assumes `"Monday"` is a function being applied to the argument `"Tuesday"`. The rest of the list of items consists of text/strings. So what the error message means is that it couldn't make a string-like thing (`Data.String.IsString`) from a function (assuming `"Monday"` is a function of type `:: Text -> Text`).

Granted, the user experience could be improved. But the view of everything as a function with well-defined types makes it easy to think through what's broken.

Let's look at a more useful/general case: calling `take` with a string instead of a number.

```haskell
ghci> D.take '5' df

<interactive>:20:8: error: [GHC-83865]
    • Couldn't match expected type ‘Int’ with actual type ‘Char’
    • In the first argument of ‘D.take’, namely ‘'5'’
      In the expression: D.take '5' df
      In an equation for ‘it’: it = D.take '5' df
```

This is a much clearer error message because the types are much simpler. The function expected an integer but got a character. Giving a number to the take function violates the promise you made to the function. Your agreement with the function was that you would give it an integer but instead you gave it a character.

The compiler is just keeping you honest.

## Plotting
<TODO>

## Data preparation
Data in the wild doesn't always come in a form that's easy to work with. A data analysis tool should make preparing and cleaning data easy. There are a number of common issues that data analysis tool must handle. We'll go through a few common ones and show how to deal with them in Haskell.

### Handling missing data
Data is oftentimes incomplete. Sometimes because of legitimate reasons, often times because of errors. Handling missing data is a foundational tasks in data analysis. In Haskell, potentially missing values are represented by a "wrapper" type called [`Maybe`](https://en.wikibooks.org/wiki/Haskell/Understanding_monads/Maybe).

```haskell
ghci> import qualified DataFrame as D
ghci> let df = D.fromUnnamedColumns [D.fromList [Just 1, Just 1, Nothing, Nothing], D.fromList [Just 6.5, Nothing, Nothing, Just 6.5], D.fromList [Just 3.0, Nothing, Nothing, Just 3.0]]
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
ghci> D.impute "0" (0 :: Double) df
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

<TODO>
