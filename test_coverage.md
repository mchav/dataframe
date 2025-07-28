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
  - Applying many does same transformation to all columns. DONE
  - Applying many doesn't change unrelated fields. DONE
  - Applying many fails if any of the columns are not found. DONE
  - Applying many throws exception when the function type doesn't equal. DONE

* applyWhere
  - Applies function when target column criteria is met. DONE
  - When criterion column doesn't exist throw an error. DONE
  - When target column doesn't exist throw an error. DONE
  - When the type of the criterion column doesn't exist throw an error. DONE
  - When the type of the target column doesn't exist throw an error. DONE
  - When the criterion function has the wrong type throw an error. DONE
  - When the target function has the wrong type throw an error. DONE

* derive
  - Applies function to given column and adds it to alias. DONE
  - When column doesn't exist throw an error. DONE

* applyAtIndex
  - Applies function to row at index.
  - Does nothing if index is out of range.
  - Throws an error if the column doesn't exist.

* take
  - Takes correct number of elements. DONE
  - If # elements is less n then don't change the column. DONE
  - If arg is negative then don't change the dimensions of the frame. DONE

* filter
  - Filters column as expected. DONE
  - Filter on non existent values returns dataframe with (0,0) dimensions. DONE
  - Filter on non-existent type throws exception. DONE

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
  - Groups by a column if at exist and other columns are vectors of vectors DONE
  - Groups by a number of columns if they exist and other columns are vectors of vectors DONE
  - If any column doesn't exist throw an error. DONE

* reduceBy
  - Reduces by a vector column
  - Throws an exception when the column doesn't exist.
  - Throws an error when the wrong type is passed into the function
  - Throws an error when the vector is of the wrong type.

* parseDefault
  - unsigned integer defaults to int
  - decimal point number defaults to double.
  - Fallback to text.

* sortBy
  - Sorts by a given column in ascending order. DONE
  - Sorts by a given column in descending order. DONE
  - Sorts by multiple columns in ascending order.
  - Sorts by multiple columns in descending order.
  - Throws an error if it doesn't exist. DONE

* describeColumns
  - Return correct types and lengths.

## Plotting
<TODO>

## CSV I/O
<TODO>
