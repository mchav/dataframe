# Using dataframe in a standalone script

What you’ll learn
* How to write a small, compiled Haskell program that reads a CSV, engineers features, imputes missing values, and filters rows.
* How the FrameM monad lets you update a dataframe without threading the df variable by hand.
* How Template Haskell column declarations eliminate stringly‑typed references.


## Generating our dataframe expressions

```haskell
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}

module Main where

import qualified DataFrame as D
import qualified DataFrame.Functions as F

import DataFrame.Monad

import Data.Text (Text)
import DataFrame.Functions ((.&&), (.>=))

-- Generate top-level column variables from a CSV file.
-- Example: 'median_house_value', 'total_bedrooms', etc.
$(F.declareColumnsFromCsvFile "./data/housing.csv")
```

`declareColumnsFromCsvFile` runs at compile time and generates top‑level, typed bindings for each column in the CSV header in snake case. In practice you get values like `median_house_value :: Expr Double` and `ocean_proximity :: Expr Text`. In your scripts you write `median_house_value .>= 500000` instead of `F.col @Double "median_house_value" .>= 500000`. This removes the possibility of misspelling column names or misspecifying the type!

Because Template Haskell reads the file at compile time, make sure `./data/housing.csv` exists relative to the project root when building, and list it under extra-source-files or data-files in your Cabal file so CI builds can find it.

## The FrameM monad
This isn't another monad tutorial. In fact, I don't think you have to know what they are to use them well. We use them in scripts because they help us keep the code compact. Suppose we wanted to:

* define a new feature called `is_expensive` which has a value of true when the house price is greater than or equal to `500_000` and is false otherwise.
* define a new feature for the average number of rooms in each house called `rooms_per_household`. 
* we also wanted to impute the mean value of total bedrooms in places where it's `Nothing`/`Null`.
* filter all rows where `is_expensive` is true, `rooms_per_household` is greater than or equal to 7, and `total_bedrooms` is greater than or equal to 200.

We'd normally have to write:

```haskell
main :: IO ()
main = do
    df <- D.readCsv "./data/housing.csv"
    let (isExpensive, dfWithExpensive) = D.deriveWithExpr "is_expensive" (median_house_value .>= 500000)
        (roomsPerHoushold, dfWithRoomsPerHousehold) = D.deriveWithExpr "rooms_per_household" (total_rooms / households)
        meanBedrooms = D.meanMaybe total_bedrooms dfWithRoomsPerHousehold
        dfWithTotalBedroomsImputed = D.impute total_bedrooms meanBedrooms dfWithRoomsPerHousehold

    print $ dfWithTotalBedroomsImputed |> D.filterWhere (isExpensive .&& roomsPerHousehold .>= 7 .&& (F.col @Double "total_bedrooms") .>= 200)
```

Because our dataframe is immutable and we return a new modified dataframe after every call to `deriveWithExpr` we get an explosion of names and generally unclear code. There are a number of ways we can solve this problem but a monadic interface seems to fit naturally here. We can make the dataframes implicit and keep chaining them through the computations.

```haskell
let df' = execFrameM df $ do
            isExpensive   <- deriveM "is_expensive" (median_house_value .>= 500000)
            roomsPerHousehold <- deriveM "rooms_per_household" (total_rooms / households)
            meanBedrooms   <- inspectM (D.meanMaybe total_bedrooms)
            totalBedrooms  <- imputeM total_bedrooms meanBedrooms
            filterWhereM (isExpensive .&& roomsPerHousehold .>= 7 .&& totalBedrooms .>= 200)
```

The code is now more readable. Your eyes can zero in on the computations without being distracted ny state keeping. We introduce a couple of functions to work within this monadic context:

* Our core functions now have a version suffixed with `M` to indicate that they are working within the FrameM monad. The monadic API is only defined for functions that may mutate at most one column schema. Functions like `groupBy + aggregate` are note supported.
* An `inspectM` function that takes a nother function that expects a dataframe and produces a value. We used this to get the mean in this case.

FrameM is an implementation of a [state monad](https://wiki.haskell.org/State_Monad) and its function names are meant to mirror the same naming scheme.

* `execFrameM df action :: DataFrame` - returns the final dataframe (like execState).
Use when you only care about the resulting table.
* `runFrameM df action :: (a, DataFrame)` - returns result value(s) and the final dataframe (like runState). Use when you want to pull out expressions or inspected values and keep the updated frame.
* `evalFrameM df action :: a` - returns just the result value(s) (like evalState). Useful when you only need scalars/Exprs from the computation.


We can extract expressions and use them outside of the monadic API as follows:

```haskell
main :: IO ()
main = do
  df <- D.readCsv "./data/housing.csv"

  let ((isExpensive, totalBedrooms), df') =
        runFrameM df $ do
          is_expensive  <- deriveM "is_expensive" (median_house_value .>= 500000)
          meanBedrooms  <- inspectM (D.meanMaybe total_bedrooms)
          totalBedrooms <- imputeM total_bedrooms meanBedrooms
          -- Return the Exprs we want to reuse outside the monad:
          pure (is_expensive, totalBedrooms)

  print $
    df'
      |> D.filterWhere (isExpensive .&& totalBedrooms .>= 200)
```

This pattern lets you keep most of your pipeline in the monad (ergonomic state updates), but allows you yo reuse the derived Exprs elsewhere - for example, in plotting, or further filtering with the non‑monadic API.

## Type safety, schema evolution, and the “foot‑gun”

What’s type‑safe here?
* Expressions are typed: `median_house_value .>= 500000` won’t compile if `median_house_value` isn’t numeric.
`ocean_proximity .&& median_house_value .>= 500000` won’t compile because `.&&` expects a Bool on both sides.
* Operators are typed: `(.>=)` expects both sides to be numeric Exprs of compatible type; `(.&&)` expects boolean Exprs.

Where is it not type‑safe?
* DataFrame schema isn’t in the type: Expressions are name‑based (e.g "median_house_value"), not tied to a specific DataFrame at the type level. This is deliberate: it enables schema evolution (you can add/remove/rename columns without changing every type signature).
* Foot‑gun: You can accidentally apply an Expr built with one frame to a different dataframe that’s missing that column (or has it with a different meaning/type). That error won’t be caught at compile time.

Practical mitigations
* Keep related steps inside one FrameM block and finish with execFrameM when possible.
* If you must reuse Exprs outside, pass the updated df' and the Exprs together (use runFrameM instead of the other two evalFrameM) to remind yourself they're from the same logical schema version.
* Normalize and rename columns immediately after ingest so names are stable throughout the pipeline.
* Consider module‑scoped helpers that build the “contract” expressions in one place (e.g a function that returns a record of Exprs) so evolution is centralized.

## Optional: Doing it without Template Haskell

If you don’t want to depend on compile‑time CSV introspection, you can define columns manually:

```haskell

let median_house_value = F.col @Double "median_house_value"
let total_bedrooms     = F.col @(Maybe Double) "total_bedrooms"
let ocean_proximity    = F.col @Text   "ocean_proximity"

execFrameM df $ do
  expensive <- deriveM "is_expensive" (median_house_value .>= 500000)
  muBeds    <- inspectM (D.meanMaybe total_bedrooms)
  bedsImp   <- imputeM total_bedrooms muBeds
  filterWhereM (bedsImp .>= 200 .&& expensive)
```
