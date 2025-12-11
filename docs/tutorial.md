# DataFrame (Haskell) — Comprehensive Tutorial & Cookbook

> A hands‑on, copy‑paste‑friendly tour of the **dataframe** ecosystem.

> **Conventions**
>
> * Code blocks are minimal and strongly typed; feel free to replace strings/paths.
> * Examples use **Iris** and **California Housing** datasets.

---

## 0) Quickstart

### Install


#### REPL

**Cabal**

```bash
$ cabal update
$ cabal install dataframe
$ dataframe
```

#### Project dependency

In your package.yaml or <project>.cabal file

```yaml
dependencies:
  - dataframe
```

#### IHaskell (Notebook)

```bash
$ git clone github.com/mchav/ihaskell-dataframe/
$ cd ihaskell-dataframe
$ sudo make up
```

---

## 1) Core Concepts

* **DataFrame**: A dataframe is a two-dimensional, table-like data structure that organizes data into rows and columns, similar to a spreadsheet or an SQL table. Columns are lazily intialized; many ops are fusion‑friendly.
* **Column / TypedColumn a**: statically typed column with phantom type `a` (e.g., `Double`, `Text`, `Bool`).
* **Expr a**: a typed DSL for building transformations safely (`zScore`, `pow`, `ifThenElse`, `abs`, `log`, `percentile`, etc.).
* **Schema**: mapping from column name → type. In many places inferred; can be annotated for safety.

Minimal imports (adjust to your module layout):

```haskell
import qualified DataFrame as D
import qualified DataFrame.Functions as F

import DataFrame ((|>))
import DataFrame.Functions ((.==), (.=), (.>), (.<), (.>=), (.<=), (.&&), (.||), as)
```

---

## 2) Loading & Saving Data

### CSV / TSV

```haskell
main :: IO ()
main = do
  dfIris <- D.readCsv "./data/iris.csv"
  print (D.dimensions dfIris)  -- (rows, cols)

  -- Save a filtered slice
  let small = D.take 5 dfIris
  D.writeCsv "out/iris_head.csv" small
```

### Parquet

```haskell
main :: IO ()
    df <- D.readParquet "data/iris.parquet"
    print df
```

---

## 3) Inspecting & Selecting

```haskell
D.dimensions df               -- (nRows, nCols)
D.nRows
D.nColumns
D.describeColumns df          -- Shows column types and null counts
D.take 10 df
D.takeLast 5 df

-- Select & rename
let df2 = D.select ["sepal.length","sepal.width","variety"] df
let df3 = D.renameMany [ ("sepal.length","sepal_len")
                       , ("sepal.width" ,"sepal_wid") ] df2

-- Drop columns
let df4 = D.exclude ["id","unused"] df

-- Reorder columns
let df5 = D.select ["variety","sepal_len","sepal_wid"] df3
```

---

## 4) Filtering Rows

```haskell
-- Basic predicates (typed)
let pLen  = F.col @Double "petal.length"
let pWid  = F.col @Double "petal.width"
let spec  = F.col @Text   "variety"

let isVersicolor = spec .== "Versicolor"
let longPetal    = pLen .> 4.5
let narrowPetal  = pWid .< 1.3

let dfV = D.filterWhere isVersicolor df
let dfC = D.filterWhere (longPetal .&& narrowPetal) df
```

---

## 5) Creating / Mutating Column

```haskell
-- Column from a list (or any foldable structure)
let df = D.insert "age" [10,30,40,50] D.empty
-- From a vector
import qualified Data.Vector as V
let d2 = D.insert "age" (V.fromList [10,30,40,50]) D.empty
-- Unboxed vectors aren't foldable so they get their own function.
import qualified Data.Vector.Unboxed as VU
let df3 = D.insertUnboxedVector "age" (VU.fromList [10,30,40,50])
-- Insert a column with less items than rows in the dataframe and have the
-- tail appear as a default value.
-- In the example below, Nyasha gets a grade of 0.
let df4 = D.insertWithDefault 0 "grades" [90,10] (D.fromNamedColumns [("Student", D.fromList ["Sizwe", "Tendai", "Nyasha"])])

-- Build expressions
let sl = F.col @Double "sepal.length"
let sw = F.col @Double "sepal.width"
let pl = F.col @Double "petal.length"
let pw = F.col @Double "petal.width"

let ratio = sl / sw
let area  = pl * pw
let wide  = D.ifThenElse (sw .> 3.0) True False -- or just (sw .> 3.0)
let z_pl  = D.zScore pl
let pw4   = D.pow pw 2

-- mutate/add
let df' = df
            |> D.deriveMany
                [
                , "ratio" .= ratio
                , "area"  .= area
                , "wide"  .= wide
                , "z_petal_length" .= z_pl
                , "pw4" .= pw4
                ]

-- Alternatively
let df2 = df
            |> D.deriveMany
                [
                , ratio `as` "ratio"
                , area `as` "area"
                , wide `as` "wide"
                , z_pl `as` "z_petal_length"
                , pw4 `as` "pw4"
                ]
```

Built‑ins commonly available: `abs`, `sqrt`, `log1p`, `exp`, `sin`, `cos`, `relu`, `signum`.
Statistical functions in expressions: `percentile k expr`, `mean expr`, `stddev expr` etc.

**User‑Defined Functions (UDFs)**

```haskell
-- Pure UDF on Doubles
let myScore x y = (x - y) / (abs y + 1e-6)
-- Supports binary functions (lift2) and unary functions (lift)
let scoreExpr   = F.lift2 myScore sl sw
let df2 = D.derive "score" scoreExpr df
```

---

## 6) Aggregations & GroupBy

```haskell
let bySpecies = D.groupBy ["species"] df'
let stats = bySpecies |>
                D.aggregate bySpecies
                    [ "n"     .= F.count pl
                    , "meanPL".= F.mean pl
                    , "sdPL"  .= F.stddev pl
                    , "minPW" .= F.mininum pw
                    , "maxPW" .= F.maximum pw
                    ]
print stats  -- pretty table to terminal
```

---

## 7) Joins

```haskell
let joined = D.innerJoin [F.col @Double "id"] dfLeft dfRight
```

Only inner join is currently supported.

---

## 8) Missing Data

```haskell
let hasNA   = not (D.selectBy [D.byProperty D.hasMissing] df == D.empty)
let dfNoNA  = D.filterAllJust df  -- drop rows with any NA
let dfImput = D.impute (F.col @Double "sepal.length") 10 df
```

---

## 9) Sorting, Shuffling, Distinct, Sampling

```haskell
D.sortBy [D.Asc "variety"] df
D.sample (mkStdGen 42) 0.1 df           -- 10% uniform random sample sample
D.shuffle (mStdGen 42) df
```

---

## 10) Split / Combine

```haskell
let (train, test) = D.randomSplit (mkStdGen 42) 0.8 (df |> D.insert "index" [1..(fst (D.dimensions df))])
let original = train <> test |> D.sortBy [D.Asc "index"]
```

---

## 11) Visualization

### Granite (Terminal Plots)

```haskell
D.plotHistogram "petal.length" df
D.plotScatter "sepal.length" "sepal.width" df
D.plotStackedBars "variety" ["sepal.width"] stats
```
