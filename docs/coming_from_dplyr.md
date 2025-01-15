# Coming from dplyr

This tutorial will walk through the examples in dplyr's [mini tutorial](https://dplyr.tidyverse.org/) showing how concepts in dplyr map to dataframe.

## Filtering
Filtering looks similar in both libraries.

```r
starwars %>% 
  filter(species == "Droid")
#> # A tibble: 6 × 14
#>   name   height  mass hair_color skin_color  eye_color birth_year sex   gender  
#>   <chr>   <int> <dbl> <chr>      <chr>       <chr>          <dbl> <chr> <chr>   
#> 1 C-3PO     167    75 <NA>       gold        yellow           112 none  masculi…
#> 2 R2-D2      96    32 <NA>       white, blue red               33 none  masculi…
#> 3 R5-D4      97    32 <NA>       white, red  red               NA none  masculi…
#> 4 IG-88     200   140 none       metal       red               15 none  masculi…
#> 5 R4-P17     96    NA none       silver, red red, blue         NA none  feminine
#> # ℹ 1 more row
#> # ℹ 5 more variables: homeworld <chr>, species <chr>, films <list>,
#> #   vehicles <list>, starships <list>
```

```haskell
starwars |> D.filter "species" (("Droid" :: Str.Text) ==)
         |> D.take 10
```

```
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
index |  name  |  height   |   mass    | hair_color | skin_color  | eye_color | birth_year | sex  |  gender   | homeworld | species |                                                                   films                                                                   |  vehicles  | starships 
------|--------|-----------|-----------|------------|-------------|-----------|------------|------|-----------|-----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------|------------|-----------
 Int  |  Text  | Maybe Int | Maybe Int |    Text    |    Text     |   Text    | Maybe Int  | Text |   Text    |   Text    |  Text   |                                                                   Text                                                                    | Maybe Text | Maybe Text
------|--------|-----------|-----------|------------|-------------|-----------|------------|------|-----------|-----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------|------------|-----------
0     | C-3PO  | Just 167  | Just 75   | NA         | gold        | yellow    | Just 112   | none | masculine | Tatooine  | Droid   | A New Hope, The Empire Strikes Back, Return of the Jedi, The Phantom Menace, Attack of the Clones, Revenge of the Sith                    | Nothing    | Nothing   
1     | R2-D2  | Just 96   | Just 32   | NA         | white, blue | red       | Just 33    | none | masculine | Naboo     | Droid   | A New Hope, The Empire Strikes Back, Return of the Jedi, The Phantom Menace, Attack of the Clones, Revenge of the Sith, The Force Awakens | Nothing    | Nothing   
2     | R5-D4  | Just 97   | Just 32   | NA         | white, red  | red       | Nothing    | none | masculine | Tatooine  | Droid   | A New Hope                                                                                                                                | Nothing    | Nothing   
3     | IG-88  | Just 200  | Just 140  | none       | metal       | red       | Just 15    | none | masculine | NA        | Droid   | The Empire Strikes Back                                                                                                                   | Nothing    | Nothing   
4     | R4-P17 | Just 96   | Nothing   | none       | silver, red | red, blue | Nothing    | none | feminine  | NA        | Droid   | Attack of the Clones, Revenge of the Sith                                                                                                 | Nothing    | Nothing   
5     | BB8    | Nothing   | Nothing   | none       | none        | black     | Nothing    | none | masculine | NA        | Droid   | The Force Awakens                                                                                                                         | Nothing    | Nothing
```

## Selecting columns
Select looks similar except in Haskell we take as argument a list of strings instead of a mix of predicates and strings.

```r
starwars %>% 
  select(name, ends_with("color"))
#> # A tibble: 87 × 4
#>   name           hair_color skin_color  eye_color
#>   <chr>          <chr>      <chr>       <chr>    
#> 1 Luke Skywalker blond      fair        blue     
#> 2 C-3PO          <NA>       gold        yellow   
#> 3 R2-D2          <NA>       white, blue red      
#> 4 Darth Vader    none       white       yellow   
#> 5 Leia Organa    brown      light       brown    
#> # ℹ 82 more rows
```

To get the same predicate-like functionality we use `selectBy`.

```haskell
starwars |> D.selectBy (\cname -> cname == "name" || T.isSuffixOf "color" cname)
         |> D.take 10
```


```
--------------------------------------------------------------------
index |        name        |  hair_color   | skin_color  | eye_color
------|--------------------|---------------|-------------|----------
 Int  |        Text        |     Text      |    Text     |   Text   
------|--------------------|---------------|-------------|----------
0     | Luke Skywalker     | blond         | fair        | blue     
1     | C-3PO              | NA            | gold        | yellow   
2     | R2-D2              | NA            | white, blue | red      
3     | Darth Vader        | none          | white       | yellow   
4     | Leia Organa        | brown         | light       | brown    
5     | Owen Lars          | brown, grey   | light       | blue     
6     | Beru Whitesun Lars | brown         | light       | blue     
7     | R5-D4              | NA            | white, red  | red      
8     | Biggs Darklighter  | black         | light       | brown    
9     | Obi-Wan Kenobi     | auburn, white | fair        | blue-gray
```

## Transforming columns

R has a general mutate function that takes in a mix of expressions and column names.

```r
starwars %>% 
  mutate(name, bmi = mass / ((height / 100)  ^ 2)) %>%
  select(name:mass, bmi)
#> # A tibble: 87 × 4
#>   name           height  mass   bmi
#>   <chr>           <int> <dbl> <dbl>
#> 1 Luke Skywalker    172    77  26.0
#> 2 C-3PO             167    75  26.9
#> 3 R2-D2              96    32  34.7
#> 4 Darth Vader       202   136  33.3
#> 5 Leia Organa       150    49  21.8
#> # ℹ 82 more rows
```

Our logic is more explicit about what's going on. Because both our fields are nullable/optional we have to specify the type.

```haskell
bmi (w :: Int) (h :: Int) = (fromIntegral w) / (fromIntegral h / 100) ** 2 :: Double

starwars
  |> D.selectRange ("name", "mass")
  -- mass and height are optionals so we combine them with
  -- Haskell's Applicative operators.
  |> D.deriveFrom (["mass", "height"], D.func (\w h -> bmi <$> w <*> h)) "bmi" 
  |> D.take 10
```

```
-------------------------------------------------------------------------------
index |         name          |  height   |   mass    |           bmi          
------|-----------------------|-----------|-----------|------------------------
 Int  |         Text          | Maybe Int | Maybe Int |      Maybe Double      
------|-----------------------|-----------|-----------|------------------------
0     | Luke Skywalker        | Just 172  | Just 77   | Just 26.027582477014604
1     | C-3PO                 | Just 167  | Just 75   | Just 26.89232313815483 
2     | R2-D2                 | Just 96   | Just 32   | Just 34.72222222222222 
3     | Darth Vader           | Just 202  | Just 136  | Just 33.33006567983531 
4     | Leia Organa           | Just 150  | Just 49   | Just 21.77777777777778 
5     | Owen Lars             | Just 178  | Just 120  | Just 37.87400580734756 
6     | Beru Whitesun Lars    | Just 165  | Just 75   | Just 27.548209366391188
7     | R5-D4                 | Just 97   | Just 32   | Just 34.009990434690195
8     | Biggs Darklighter     | Just 183  | Just 84   | Just 25.082863029651524
9     | Obi-Wan Kenobi        | Just 182  | Just 77   | Just 23.24598478444632 
```

Haskell's applicative syntax does take some getting used to.

`f <$> a` means apply f to the thing inside the "container". In this
case the container (or more infamously the monad) is of type `Maybe`.
So this can also be written as `fmap f a`.

But this only works if our `f` takes a single argument. If it takes
two arguments then the we use `<*>` to specify the second argument.

So, applying bmi to two optionals can be written as:

```haskell
ghci> fmap (+) (Just 2) <*> Just 2
Just 4
ghci> (+) <$> Just 2 <*> Just 2
Just 4
```

You'll find a wealth of functions for dealing with optionals in the package
`Data.Maybe`.

## Sorting

```r
starwars %>% 
  arrange(desc(mass))
#> # A tibble: 87 × 14
#>   name      height  mass hair_color skin_color eye_color birth_year sex   gender
#>   <chr>      <int> <dbl> <chr>      <chr>      <chr>          <dbl> <chr> <chr> 
#> 1 Jabba De…    175  1358 <NA>       green-tan… orange         600   herm… mascu…
#> 2 Grievous     216   159 none       brown, wh… green, y…       NA   male  mascu…
#> 3 IG-88        200   140 none       metal      red             15   none  mascu…
#> 4 Darth Va…    202   136 none       white      yellow          41.9 male  mascu…
#> 5 Tarfful      234   136 brown      brown      blue            NA   male  mascu…
#> # ℹ 82 more rows
#> # ℹ 5 more variables: homeworld <chr>, species <chr>, films <list>,
#> #   vehicles <list>, starships <list>
```

```haskell
starwars |> D.sortBy D.Descending ["mass"] |> D.take 5
```

```
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
index |         name          |  height   |   mass    | hair_color |    skin_color    |   eye_color   | birth_year |      sex       |  gender   | homeworld | species |                                    films                                     |              vehicles              |            starships           
------|-----------------------|-----------|-----------|------------|------------------|---------------|------------|----------------|-----------|-----------|---------|------------------------------------------------------------------------------|------------------------------------|--------------------------------
 Int  |         Text          | Maybe Int | Maybe Int |    Text    |       Text       |     Text      | Maybe Int  |      Text      |   Text    |   Text    |  Text   |                                     Text                                     |             Maybe Text             |           Maybe Text           
------|-----------------------|-----------|-----------|------------|------------------|---------------|------------|----------------|-----------|-----------|---------|------------------------------------------------------------------------------|------------------------------------|--------------------------------
0     | Jabba Desilijic Tiure | Just 175  | Just 1358 | NA         | green-tan, brown | orange        | Just 600   | hermaphroditic | masculine | Nal Hutta | Hutt    | A New Hope, Return of the Jedi, The Phantom Menace                           | Nothing                            | Nothing                        
1     | Grievous              | Just 216  | Just 159  | none       | brown, white     | green, yellow | Nothing    | male           | masculine | Kalee     | Kaleesh | Revenge of the Sith                                                          | Just "Tsmeu-6 personal wheel bike" | Just "Belbullab-22 starfighter"
2     | IG-88                 | Just 200  | Just 140  | none       | metal            | red           | Just 15    | none           | masculine | NA        | Droid   | The Empire Strikes Back                                                      | Nothing                            | Nothing                        
3     | Tarfful               | Just 234  | Just 136  | brown      | brown            | blue          | Nothing    | male           | masculine | Kashyyyk  | Wookiee | Revenge of the Sith                                                          | Nothing                            | Nothing                        
4     | Darth Vader           | Just 202  | Just 136  | none       | white            | yellow        | Nothing    | male           | masculine | Tatooine  | Human   | A New Hope, The Empire Strikes Back, Return of the Jedi, Revenge of the Sith | Nothing                            | Just "TIE Advanced x1"
```

## Grouping and aggregating

```r
starwars %>%
  group_by(species) %>%
  summarise(
    n = n(),
    mass = mean(mass, na.rm = TRUE)
  ) %>%
  filter(
    n > 1,
    mass > 50
  )
#> # A tibble: 9 × 3
#>   species      n  mass
#>   <chr>    <int> <dbl>
#> 1 Droid        6  69.8
#> 2 Gungan       3  74  
#> 3 Human       35  81.3
#> 4 Kaminoan     2  88  
#> 5 Mirialan     2  53.1
#> # ℹ 4 more rows
```

```haskell
starwars |> D.select ["species", "mass"]
         |> D.groupByAgg D.Count ["species"]
         |> D.reduceByAgg D.Mean "mass"
         -- Always better to be explcit about types for
         -- numbers but you can also turn on defaults
         -- to save keystrokes.
         |> D.filter "Count" ((1::Int)<)
         |> D.filter "Mean_mass" ((50 ::Double)<)
```

```
--------------------------------------------
index | species  |     Mean_mass     | Count
------|----------|-------------------|------
 Int  |   Text   |      Double       |  Int 
------|----------|-------------------|------
0     | Human    | 81.47368421052632 | 35   
1     | Droid    | 69.75             | 6    
2     | Wookiee  | 124.0             | 2    
3     | NA       | 81.0              | 4    
4     | Gungan   | 74.0              | 3    
5     | Zabrak   | 80.0              | 2    
6     | Twi'lek  | 55.0              | 2    
7     | Kaminoan | 88.0              | 2
```
