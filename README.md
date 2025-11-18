<h1 align="center">
  <a href="https://dataframe.readthedocs.io/en/latest/">
    <img width="100" height="100" src="https://raw.githubusercontent.com/mchav/dataframe/master/docs/_static/haskell-logo.svg" alt="dataframe logo">
  </a>
</h1>

<div align="center">
  <a href="https://hackage.haskell.org/package/dataframe">
    <img src="https://img.shields.io/hackage/v/dataframe" alt="hackage Latest Release"/>
  </a>
  <a href="https://github.com/mchav/dataframe/actions/workflows/haskell-ci.yml">
    <img src="https://github.com/mchav/dataframe/actions/workflows/haskell-ci.yml/badge.svg" alt="C/I"/>
  </a>
</div>

<p align="center">
  <a href="https://dataframe.readthedocs.io/en/latest/">User guide</a>
  |
  <a href="https://discord.gg/8u8SCWfrNC">Discord</a>
</p>

# DataFrame

A fast, safe, and intuitive DataFrame library.

## Why use this DataFrame library?

* Encourages concise, declarative, and composable data pipelines.
* Static typing makes code easier to reason about and catches many bugs at compile time—before your code ever runs.
* Delivers high performance thanks to Haskell’s optimizing compiler and efficient memory model.
* Designed for interactivity: expressive syntax, helpful error messages, and sensible defaults.
* Works seamlessly in both command-line and notebook environments—great for exploration and scripting alike.

## Features
- Type-safe column operations with compile-time guarantees
- Familiar, approachable API designed to feel easy coming from other languages.
- Interactive REPL for data exploration and plotting.

## Quick start
Browse through some examples in [binder](https://mybinder.org/v2/gh/mchav/ihaskell-dataframe/HEAD) or in our [playground](https://ulwazi-exh9dbh2exbzgbc9.westus-01.azurewebsites.net/lab).

## Install

### Cabal
To use the CLI tool:
```bash
$ cabal update
$ cabal install dataframe
$ dataframe
```

As a prodject dependency add `dataframe` to your <project>.cabal file.

### Stack (in stack.yaml add to extra-deps if needed)
Add to your package.yaml dependencies:
```yaml
dependencies:
  - dataframe
```

Or manually to stack.yaml extra-deps if needed.

## Example

```haskell
dataframe> df = D.fromNamedColumns [("product_id", D.fromList [1,1,2,2,3,3]), ("sales", D.fromList [100,120,50,20,40,30])]
dataframe> df
------------------
product_id | sales
-----------|------
   Int     |  Int 
-----------|------
1          | 100  
1          | 120  
2          | 50   
2          | 20   
3          | 40   
3          | 30   

dataframe> :exposeColumns df
"product_id :: Expr Int"
"sales :: Expr Int"
dataframe> df |> D.groupBy [F.name product_id] |> D.aggregate [F.sum sales `as` "total_sales"]
------------------------
product_id | total_sales
-----------|------------
   Int     |     Int    
-----------|------------
1          | 220        
2          | 70         
3          | 70         
```

## Documentation
* 📚 User guide: https://dataframe.readthedocs.io/en/latest/
* 📖 API reference: https://hackage.haskell.org/package/dataframe/docs/DataFrame.html
