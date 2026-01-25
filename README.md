# Daniel Contribution

## Execution Steps


## Directory Explanation

### data
The data directory serves as the central repository for all things data. Here contains the csv files that
will build the graphs that will be tested upon, both the artificial ones and the annotated subsections of
the LDBC dataset

### graph_init_sql
This directory contains a host of .sql files that, when executed using the shell script included in the
logic directory, will build and populate the graphs defined by the .csv files in the data directory

### queries
This directory contains the queries that will be tested across

### logic
This directory contains all the code that defines my contribution

#### Setup
The setup directory contains all the code necessary to set the stage for query assessment

* **Annotator.py** takes in the specified LDBC data and annotates it according to either the dewey / string strategy or the integer range / pre-post strategy
* **ArtificialTreeGenerator.py** Generates and annotates the artificial single tree environments that were previously tested on. It builds .csv files to represent those graphs and saves them to their appropriate place in the data directory. 
* **CreateDBBuilderSQL.py** generates .sql files that will build up the data in the data directory into ApacheAGE graphs.


## Misc Notes
* The ldbc_data will not be used in the final version, this is just to pull from for the creation of the other pieces of data
* Write now the repository deals with the LDBC dataset in scale factor 0.1
  * This is an area that can be improved upon, and the logic need not be altered meaningfully
* 