# Daniel Contribution

## Execution Steps

### Set up Databases (ApacheAGE)
1. Run logic/setup/setup.py
2. Run logic/setup/CreateDBBuilderSQL.py
3. Run reset_env.sh
4. Run run_sql.sh

### Test Queries
1. Run logic/query_assessment/AssessmentLogic.py

### Execution Step Notes
* These instructions are brittle for the moment. They are strictly designed for apache, and subject to issues with file paths. My two immediate concurrent goals are as follows
  * Make all operations location agnostic (all relative file paths)
  * Make all setup operations one single command
  * Setup all the same features for Kuzu
  * And of course maintain organizational clarity while doing so

## Directory Explanation

### data
The data directory serves as the central repository for all things data. Here contains the csv files that
will build the graphs that will be tested upon, both the artificial ones and the annotated subsections of
the LDBC dataset

### graph_init_sql
This directory contains a host of .sql files that, when executed using the shell script included in the
logic directory, will build and populate the graphs defined by the .csv files in the data directory. At 
the moment these are limited to strictly creating the ApacheAGE files, however as the Kuzu logic is refactored
and added into the cleaned repository the notes on this directory will be updated to reflect any changes.

### queries
This directory contains the queries that will be tested across the instances of the database. These queries differ
based on the annotation strategy associated with particular nodes, and by database system (as syntax varies slightly
between ApacheAGE, Kuzu, and Neo4j). At the moment only queries for ApacheAGE have been cleaned and placed into the 
directory, but the rest are coming. 

### logic
This directory contains all the code that defines my contribution:

#### Setup
The setup directory contains all the code necessary to set the stage for query assessment

* **Annotator.py** takes in the specified LDBC data and annotates it according to either the dewey / string strategy or the integer range / pre-post strategy
* **ArtificialTreeGenerator.py** Generates and annotates the artificial single tree environments that were previously tested on. It builds .csv files to represent those graphs and saves them to their appropriate place in the data directory. 
* **CreateDBBuilderSQL.py** generates .sql files that will build up the data in the data directory into ApacheAGE graphs.

#### Query Assessment
This file contains the logic to assess queries on the created databases as well as to properly parametrize them. 
Results of the findings derived from this section of the repository are saved in the results directory

### Results
This directory contains the two subdirectories result_logs and visualizations

#### result_logs
Contains the results of the runs. This includes time elapsed (as measured with a timer that starts when query execution 
starts and ends when the program returns to its primary execution flow), query plans as characterized with the EXPLAIN 
keyword, and the parameters that were passed to the given query. These values are collected for each run of each query. 

#### visualizations
The visualizations subdirectory yet remains empty, but will contain visualizations based on the result_log files 
described above. 


## Misc Notes
* The ldbc_data will not be used in the final version, this is just to pull from for the creation of the other pieces of data
* Write now the repository deals with the LDBC dataset in scale factor 0.1
  * This is an area that can be improved upon, and the logic need not be altered meaningfully
* 