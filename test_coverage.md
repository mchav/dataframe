# Test Coverage

## Properties
* Empty dataframe
  - Has dimensions (0, 0)
  - Has 8 empty vectors
  - No column indices

## Operations

* addColumn
  - Adding a boxed vector to an empty dataframe creates a new column boxed containing the vector elements. DONE
  - Adding a boxed vector with a boxed type (Int/Double) to an empty dataframe creates a new column unboxed containing the vector elements. DONE
  - Adding columns > initial vector size gracefully adds a column that we can retrieve. DONE
  - Adding columns > initial vector size gracefully adds a column updates dimentions. DONE
  - Adding a column with the same name as an existing column overwrites the contents. DONE
  - Adding a column with more values than the current DF dimensions throws an exception. DONE
  - Adding a column with less values than the current DF dimensions adds column with optionals. DONE

* addColumnWithDefault
  - Adding a column with less values than the current DF dimensions adds column with optionals. DONE
  - Adding a column with as many values is a no-op. DONE

* apply
  - Applying to an existing column maps function to all values. DONE
  - Applying to non-existent column throws column not found exception. DONE
  - Applying function of wrong type throws exception. DONE

* applyMany
  - Applying many does same transformation to all columns.
  - Applying many fails if any of the columns are not found.
  - Applying many throws exception when the function type doesn't equal 

* applyWhere
  - Applies function when target column criteria is met
  - When criterion column doesn't exist throw an error.
  - When the type of the target column doesn't exit throw an error.
  - When the function has the wrong type throw an error. 

* applyWithAlias
  - Applies function to given column and adds it to alias.
  - When column doesn't exist throw an error.

* applyAtIndex
  - Applies function to row at index.
  - Does nothing if index is out of range.
  - Throws an error if the column doesn't exist.

* take
  - Takes correct number of elements.
  - If # elements is less n then don't change the column.
  - If arg is negative then don't change the

* filter
  - Filters column as expected
  - Filter on non existent values returns dataframe with (0,0) dimensions. 

* valueCounts
  - Counts values as expected.
  - Throws error when column doesn't exist.

* select
  - Selects a subset of the columns on select
  - Check that dimensions update after select
  - Add new column to result of selected column
  - Updates free indices on select

* exclude
  - Drops a subset of the columns on exclude
  - Check that dimensions update after exclude
  - Add new column to result of exclude column
  - Updates free indices on exclude

* groupBy
  - Groups by a column if at exist and other columns are vectors of vectors
  - Groups by a number of columns if they exist and other columns are vectors of vectors
  - If any column doesn't exist throw an error.

* reduceBy
  - Reduces by a vector column
  - Throws an exception when the column doesn't exist.
  - Throws an error when the wrong type is passed into the function
  - Throws an error when the vector is of the wrong type.

* combine
  - Combines two unboxed columns into an unboxed column if function returns unboxed type.
  - Combines two unboxed columns into an boxed column if function returns boxed type.
  - Combines two boxed columns into an unboxed column if function returns unboxed type.
  - Combines two boxed columns into an boxed column if function returns boxed type.
  - Throws error if any of the columns doesn't exist.
  - Throws type mismatch.

* parseDefault
  - unsigned integer defaults to int,
  - decimal point number defaults to double.
  - Fallback to text.

* sortBy
  - Sorts by a given column.
  - Throws an error if it doesn't exist.

* columnInfo
  - Return correct types and lengths.

## Plotting
<TODO>

## CSV I/O
<TODO>
