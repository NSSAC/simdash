
# SimDash

A web based dashboard for visualizing simulations, SimDash allows users to easily view how a simulation has developed over time.  SimDash is especially useful for periodically extracting and visualizing data from an ongoing simulation or monitoring the status of a running process.   This information can then be viewed on a localhost dashboard through powerful Vega and Altair graphics.  

# Getting Started
## Installation
Simdash can be installed locally with [pip](https://pip.readthedocs.io/en/stable/installing/) by running:

     $ pip install simdash
     
## Quick Example
Here is an example utilizing SimDash to quickly visualize several data points and display the result onto a localhost web dashboard with just a Python file and a TOML configuration file.
In the Python file, a `Database` and `Table` tablesare easily populated with data.

	from simdash.database.database import Database
	
	the_db = Database("path_to_some_database.db")
	columns = ["logical_time", "real_time", "num_online", "num_tweets"]
	data_types = ["FLOAT", "INT", "FLOAT", "FLOAT"]
	var_types = ["Q", "T", "Q", "Q"]
	the_db.make_table("your_table", columns, data_types, var_types)
	your_table = the_db.get_table("your_table")
	your_table.append(num_online=400, num_tweets=600)
	your_table.append(num_online=500, num_tweets=700)
	your_table.append(num_online=400, num_tweets=800)

Then in a TOML configuration file named some_toml.toml, the details for the chart's encodings can be specified.
	
	[[tab]]
	table_name = "your_table"
	mark = "line"
	[tab.encode]
	x = "logical_time"
	y = "num_tweets"
	[[tab]]
	table_name = "your_table"
	mark = "line"
	[tab.encode]
	x = "real_time"
	y = "num_online"

Finally, in the command shell run the following script:

	$ simdash serve -d path_to_some_database.db -c some_toml.toml

and your visualization will appear on [localhost:8888/displayconfig](localhost:8888/displayconfig)
<a href="https://ibb.co/xHFmVLR"><img src="https://i.ibb.co/KwKNCh3/simdashboard-edited.png" alt="simdashboard-edited" border="0" /></a>

## Writing your own SimDash data pipeline
It can be incredibly tedious to individually append each of your data points to a SimDash `Table` and if this is your goal, it may make more sense to use other visualization libraries such as [Altair](https://github.com/altair-viz/altair) or [Vega](https://github.com/vega/vega).   SimDash's use comes more from its ability to work with simulations, where a simple python script can run in the background and extract data values from the simulation.  Then by running the SimDash command on the command line, you can easily view any changes in your simulation and its values.  One example of such a data pipeline is [Getpid](https://github.com/kh8fb/getpid), a data pipeline for gathering information about overall system usage as well as the memory and CPU consumption of each PID.

## Databases & Tables
`Databases` are a key component of SimDash.  They track vital information for graphics such as the data column names, their SQL datatypes, and their Altair encodings.  Databases can be created in any Python file with: 
	
	from simdash.database import database
	your_db = database.Database("file_path_to_database.db")
  
`Databases` can be used to create, access, and append to `Tables`, which hold the actual values from the data.  `Tables` have an emphasis on tracking variables with time, and thus require a logical time step and a real time column.  Each time values are appended to a `Table`, these columns can be directly specified or if they are not specified, SimDash will automatically jump to the next logical time step and record the current time :

	your_table = your_db.make_table("your_table", columns, dtypes, vtypes)
	your_table.append(column1=12, column2=15, column3="yes")

## TOML Configurations
SimDash encourages using TOML files to help configure graphics for any data that haven't been retrieved through [Getpid](https://github.com/kh8fb/getpid).   These files are written in the following fashion.  They start with an array declaration that a `Table` will be accessed.  This is followed by key specifications of the desired `mark` and which `Table` in the `Database` to pull from.
	
	[[tab]]
	table_name = "your_table"
	mark = "line"

This is followed by the graphic's encodings, which are any of [Altair's](https://altair-viz.github.io/user_guide/encoding.html) encodings but must include specifications for the x and y variables

	[tab.encode]
	x = "column1"
	y = "column2"


## Serving your visualizations
Once a database and table have been filled with data values, they are ready to be visualized.  Run the following command in the command line with `-d` specifying the path to the database file and `-c` specifying the path to the configuration file.   The host and port number can also be specified with `-h` and `-p` if something other than localhost:8888 is desired.

	simdash serve -d path_to_database.db -c path_to_config.toml -h localhost -p 8888  