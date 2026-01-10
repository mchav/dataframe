dataframe
=========

A practical DataFrame library for Haskell, focused on type-safe data wrangling and analysis.

**dataframe** lets you load tabular data and transform it with a functional, composable API. It leans on Haskell’s type system to help you catch common mistakes early (like mixing types or referencing missing columns), while still aiming to stay pleasant to use for everyday analysis.

*******************************
Why use this DataFrame library?
*******************************

**Type Safety Meets Data Analysis**

* **Catch bugs before runtime**: Statically typed expressions makes code easier to reason about and catches many bugs at compile time—before your code ever runs.
* **Expressive and composable**: Encourages concise, declarative, and composable data pipelines that read like the logic you're thinking.

**Designed for Productivity**

* **Interactive-first design**: Expressive syntax, helpful error messages, and sensible defaults make safe exploration a joy.
* **Works everywhere**: Works seamlessly in both command-line and notebook environments—great for exploration and scripting alike.
* **Familiar operations**: If you've used pandas, dplyr, or polars, you'll feel right at home.

****************
Who is this for?
****************

* **Data scientists and analysts** who want type safety with minimal cognitive/code overhead.
* **Haskell developers** building data/machine learning pipelines or analytics applications

Tutorials
---------
*Learn by doing—step-by-step lessons to get you started.*

.. toctree::
   :maxdepth: 1

   quick_start
   tutorial
   using_dataframe_in_a_standalone_script
   exploratory_data_analysis_primer
   intro_to_probability_and_data

How-to Guides
-------------
*Practical guides for accomplishing specific tasks.*

.. toctree::
   :maxdepth: 1

   cookbook
   coming_from_other_implementations

Explanation
-----------
*Understand the concepts and design decisions.*

.. toctree::
   :maxdepth: 1

   dataframes_in_haskell
   haskell_for_data_analysis

Reference
---------
The API is documented in Hackage.
