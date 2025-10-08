dataframe
=========

A fast, safe, and intuitive DataFrame library.

*******************************
Why use this DataFrame library?
*******************************

* Encourages concise, declarative, and composable data pipelines.
* Static typing makes code easier to reason about and catches many bugs at compile time—before your code ever runs.
* Delivers high performance thanks to Haskell’s optimizing compiler and efficient memory model.
* Designed for interactivity: expressive syntax, helpful error messages, and sensible defaults.
* Works seamlessly in both command-line and notebook environments—great for exploration and scripting alike.

***************
Getting started
***************

****************
Jupyter notebook
****************

* We have a `hosted version of the Jupyter notebook <https://ulwazi-exh9dbh2exbzgbc9.westus-01.azurewebsites.net/lab>`_ on azure sites. This is hosted on Azure's free tier so it can only support 3 or 4 kernels at a time.
* To get started quickly, use the Dockerfile in the `ihaskell-dataframe <https://github.com/mchav/ihaskell-dataframe>`_ to build and run an image with dataframe integration.
* For a preview check out the `California Housing <https://github.com/mchav/dataframe/blob/main/docs/California%20Housing.ipynb>`_ notebook.

***
CLI
***

* Run the installation script :code:`curl '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/mchav/dataframe/refs/heads/main/scripts/install.sh | sh`
* Download the run script with: :code:`curl --output dataframe "https://raw.githubusercontent.com/mchav/dataframe/refs/heads/main/scripts/dataframe.sh"`
* Make the script executable: :code:`chmod +x dataframe`
* Add the script your path: :code:`export PATH=$PATH:./dataframe`
* Run the script with: :code:`dataframe`


.. toctree::
    :maxdepth: 2

    quick_start
    haskell_for_data_analysis
    coming_from_other_implementations
    exploratory_data_analysis_primer
