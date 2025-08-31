# Exercies

The following exercies are adapted from Hackerrank's SQL challenges.

## Exercise 1: Basic filtering
For this question we will use the data in `./data/city.csv`.

Query all columns for a city with the ID 1661.

### Solution
```haskell
ghci> df |> D.filterWhere (id F.== 1661)
------------------------------------------------------------
index |  id  |  name  | country_code | district | population
------|------|--------|--------------|----------|-----------
 Int  | Int  |  Text  |     Text     |   Text   |    Int    
------|------|--------|--------------|----------|-----------
0     | 1661 | Sayama | JPN          | Saitama  | 162472
```

## Exercise 2: Basic filterig (cont)
For this question we will use the data in `./data/city.csv`.

Query all columns of every Japanese city. The `country_code` for Japan is "JPN".

### Solution
```haskell
ghci> df |> D.filterWhere (country_code F.== F.lit "JPN")
---------------------------------------------------------------
index |  id  |   name   | country_code | district  | population
------|------|----------|--------------|-----------|-----------
 Int  | Int  |   Text   |     Text     |   Text    |    Int    
------|------|----------|--------------|-----------|-----------
0     | 1613 | Neyagawa | JPN          | Osaka     | 257315    
1     | 1630 | Ageo     | JPN          | Saitama   | 209442    
2     | 1661 | Sayama   | JPN          | Saitama   | 162472    
3     | 1681 | Omuta    | JPN          | Fukuoka   | 142889    
4     | 1739 | Tokuyama | JPN          | Yamaguchi | 107078
```

## Exercise 3: Basic filtering (cont)
For this question we will use the data in `./data/city.csv`.

Query all columns for all American cities in city dataframe with:
* populations larger than 100000, and
* the CountryCode for America is "USA".

## Solution
```haskell
ghci> D.readCsv "./data/country.csv"
ghci> :exposeColumns df
ghci> df |> D.filterWhere ((population F.> F.lit 100000) `F.and` (country_code F.== F.lit "USA"))
---------------------------------------------------------------------
index |  id  |     name      | country_code |  district  | population
------|------|---------------|--------------|------------|-----------
 Int  | Int  |     Text      |     Text     |    Text    |    Int    
------|------|---------------|--------------|------------|-----------
0     | 3878 | Scottsdale    | USA          | Arizona    | 202705    
1     | 3965 | Corona        | USA          | California | 124966    
2     | 3973 | Concord       | USA          | California | 121780    
3     | 3977 | Cedar Rapids  | USA          | Iowa       | 120758    
4     | 3982 | Coral Springs | USA          | Florida    | 117549
```

## Exercise 4: Constraining output
For this question we will use the data in `./data/city.csv`.

Show the first 5 rows of the dataframe.

### Solution
```haskell
ghci> df |> D.take 5
------------------------------------------------------------------------------
index | id  |       name       | country_code |     district      | population
------|-----|------------------|--------------|-------------------|-----------
 Int  | Int |       Text       |     Text     |       Text        |    Int    
------|-----|------------------|--------------|-------------------|-----------
0     | 6   | Rotterdam        | NLD          | Zuid-Holland      | 593321    
1     | 19  | Zaanstad         | NLD          | Noord-Holland     | 135621    
2     | 214 | Porto Alegre     | BRA          | Rio Grande do Sul | 1314032   
3     | 397 | Lauro de Freitas | BRA          | Bahia             | 109236    
4     | 547 | Dobric           | BGR          | Varna             | 100399
```

## Exercise 5: Basic selection
For this question we will use the data in `./data/city.csv`.

Get the first 5 names of the city names.

### Solution
```haskell
ghci> df |> D.select [F.name name] |> D.take 5
------------------------
index |       name      
------|-----------------
 Int  |       Text      
------|-----------------
0     | Rotterdam       
1     | Zaanstad        
2     | Porto Alegre    
3     | Lauro de Freitas
4     | Dobric
```

## Exercise 6: Selection and filtering
For this question we will use the data in `./data/city.csv`.

Query the names of all the Japanese cities and show only the first 5 results.


### Solution
```haskell
ghci> df |> D.filterWhere (country_code F.== F.lit "JPN") |> D.select [F.name name] |> D.take 5
----------------
index |   name  
------|---------
 Int  |   Text  
------|---------
0     | Neyagawa
1     | Ageo    
2     | Sayama  
3     | Omuta   
4     | Tokuyama
```

## Exercise 7: Basic select (cont)
For this question we will use the data in `./data/station.csv`.

Show the first five city and state rows.

### Solution
```haskell
ghci> df |> D.select [F.name city, F.name state] |> D.take 5
----------------------------
index |     city     | state
------|--------------|------
 Int  |     Text     | Text 
------|--------------|------
0     | Kissee Mills | MO   
1     | Loma Mar     | CA   
2     | Sandy Hook   | CT   
3     | Tipton       | IN   
4     | Arlington    | CO 
```

## Exercise 8: Distinct
For this question we will use the data in `./data/station.csv`.

Query a list of city names for cities that have an even ID number. Show the results in any order, but exclude duplicates from the answer.


### Solution
```haskell
ghci> df |> D.filterWhere (F.lift even id) |> D.select [F.name city] |> D.distinct 
-----------------------------
index |         city         
------|----------------------
 Int  |         Text         
------|----------------------
0     | Rockton              
1     | Forest Lakes         
2     | Yellow Pine          
3     | Mosca                
4     | Rocheport            
5     | Millville            
...
230   | Lee                  
231   | Elm Grove            
232   | Orange City          
233   | Baker                
234   | Clutier
```
